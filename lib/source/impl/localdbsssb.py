import logging
import time
import re
import collections
import pprint
import fnmatch
import sys,os
import threading
import gfal2
import datetime
import hashlib
from urllib import urlopen

from common.interface.mysql import MySQL
from common.interface.copy import CopyInterface
from common.interface.deletion import DeletionInterface
from common.interface.siteinfo import SiteInfoSourceInterface
from common.interface.replicainfo import ReplicaInfoSourceInterface
from common.interface.datasetinfo import DatasetInfoSourceInterface
from common.dataformat import Dataset, Block, File, Site, Group
from common.dataformat import DatasetReplica, BlockReplica
import common.configuration as config
from common.misc import parallel_exec

logger = logging.getLogger(__name__)

class AltSite():
    def __init__(self, intid):
        myid = intid
        kidlist = []
    def addSubject(self,obj):
        if obj not in kidlist:
            kidlist.append(obj)

class AltBaseType():
    def __init__(self):
        self.parents = {}
        self.kids = {}
        self.size4site = {}
        self.modt4site = {}
        self.complete = {}
        self.maxSize = 0
        self.maxFiles = 0
    def allParents(self):
        return self.parents.keys()
    def getParent(self,name):
        if name in self.parents: return self.parents[name]
        else:                    return None
    def addParent(self,name,obj):
        self.parents[name] = obj
    def allKids(self):
        return self.kids.keys()
    def getKid(self,name):
        if name in self.kids: return self.kids[name]
        else:                 return None
    def addKid(self,name,obj):
        self.kids[name] = obj
    def addLocation(self, siteInd, size = 0, modt = 0):
        if siteInd not in self.size4site:
            self.size4site[siteInd] = size
        if siteInd not in self.modt4site:
            self.modt4site[siteInd] = modt

    def locatedAt(self):
        return self.size4site.keys()
    def locatedAtSite(self,siteInd):
        if siteInd in self.size4site: return True
        return False
    def sizeAtSite(self,siteInd):
        return self.size4site[siteInd]
    def getMaxSize(self):
        if self.maxSize != 0:
            return self.maxSize
        for si in self.size4site:
            if self.size4site[si] > self.maxSize:
                self.maxSize = self.size4site[si]
        return self.maxSize
    def getMaxNfiles(self):
        if self.maxFiles != 0:
            return self.maxFiles
        for si in self.size4site:
            if len(self.kidsAtSite(si)) > self.maxFiles:
                self.maxFiles = len(self.kidsAtSite(si))
        return self.maxFiles

    def isComplete(self,siteInd):
        if siteInd in self.complete:
            return self.complete[siteInd]

        allkids = self.kidsAtSite(siteInd)
        self.complete[siteInd] = 0
        if len(allkids) == self.getMaxNfiles():
            self.complete[siteInd] = 1
            for kid in allkids:
                if not kid.isComplete(siteInd):
                    self.complete[siteInd] = 0
                    break
        return self.complete[siteInd]

    def kidsAtSite(self,siteInd):
        temp = []
        for kid in self.kids:
            if self.kids[kid].locatedAtSite(siteInd):
                temp.append(self.kids[kid])
        return temp
    def lastUpdated(self,siteInd):
        return self.modt4site[siteInd]

    def updateStats(self):
        for kid in self.kids:
            kidObj = self.kids[kid]
            kidObj.updateStats()
            kidSites = kidObj.locatedAt()
            for si in kidSites:
                if si not in self.size4site:
                    self.size4site[si] = 0
                self.size4site[si] += kidObj.sizeAtSite(si)

                if si not in self.modt4site:
                    self.modt4site[si] = 0
                if self.modt4site[si] < kidObj.lastUpdated(si):
                    self.modt4site[si]  = kidObj.lastUpdated(si)

class AltDataset(AltBaseType):
    def __init__(self):
        AltBaseType.__init__(self)
        del self.parents

class AltBlock(AltBaseType):
    def __init__(self):
        AltBaseType.__init__(self)

    @staticmethod
    def convert2md5(name_str):
        m = hashlib.md5()
        m.update(name_str)
        retval = m.hexdigest()
        del m
        return retval
    
class AltFile(AltBaseType):
    def __init__(self):
        AltBaseType.__init__(self)
        self.maxFiles = 1
        del self.kids
    def updateStats(self):
        pass
    def kidsAtSite(self,siteInd):
        return []
    def isComplete(self,siteInd):
        if siteInd in self.complete:
            return self.complete[siteInd]
        if self.size4site[siteInd] != self.getMaxSize():
            self.complete[siteInd] = 0
        else:
            self.complete[siteInd] = 1
        return self.complete[siteInd]


class LocalDBSSSB(CopyInterface, DeletionInterface, SiteInfoSourceInterface, ReplicaInfoSourceInterface, DatasetInfoSourceInterface):
    """
    Interface to local/DBS/SSB using datasvc REST API.
    """

    def __init__(self):
        CopyInterface.__init__(self)
        DeletionInterface.__init__(self)
        SiteInfoSourceInterface.__init__(self)
        ReplicaInfoSourceInterface.__init__(self)
        DatasetInfoSourceInterface.__init__(self)
        self._mysql = MySQL(**config.mysqlregistry.db_params)

    def schedule_copy(self, dataset_replica, group, comments = '', is_test = False): 
#override (CopyInterface)
        pass
        
    def copy_file(self, source, destination):
        attempts = 1
        while(True):
            ctx = gfal2.creat_context()
            params = ctx.transfer_parameters()
            params.overwrite = True
            params.create_parent = True
            params.timeout = 300

            try:
                print source
                print " attempt = " + str(attempts)
                r = ctx.filecopy(params, source, destination)
                del ctx
                break
            except Exception, e:
                print " !!!!!!!!!!!!---------!!!!!!!!!!!!"
                print "Copy failed: %s" % str(e)
                del ctx
                attempts += 1
        
        print ".. copied "

    def form_sqlinsert(self,table,names,values):
        sql = "INSERT INTO " + table + " ("
        for ii in names[:-1]: sql = sql + ii + ","
        sql = sql + names[-1] + ") values("

        for ii in values[:-1]: sql= sql + "'" + str(ii) + "',"
        sql = sql + "'" + str(values[-1]) +"')"
        return sql


    def schedule_copies(self, replica_list, group, comments = '', is_test = False): 
#override (CopyInterface)
        #here we take user wishes and transfer them, if needed" into
        #entries for file management system (DFMS)
        nowTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        request_mapping = {}
        print "--- copying num of reps = " + str(len(replica_list))

        uninames = ['item','datatype','site','reqtype','created','updated']
        trqnames = ('reqid','file','site_from','site_to','status')
        print replica_list
        for replica in replica_list:
            dsetObj = replica.dataset
            fileList =  dsetObj.files
            targetSite = replica.site.name

            sql = "select reqid from requests_unified where item='" + dsetObj.name+"'"
            sql = sql + " and site='" + targetSite + "' and reqtype='copy'"
            print sql
            ids = self._mysql.query(sql)
            if len(ids) == 0:
                univals = [dsetObj.name,'dataset',targetSite,'copy',nowTime,nowTime]
                sql = self.form_sqlinsert("requests_unified",uninames,univals)
                print sql
                reqid = self._mysql.query(sql)
            else:
                reqid = ids[0]

            requests = []
            for repl in dsetObj.replicas:
                if repl == replica:
                    continue
                if not repl.is_complete:
                    continue
                sourceSite = repl.site.name
                for fileObj in fileList:
                    requests.append((reqid,fileObj.fullpath(),sourceSite,targetSite,'new'))
                
            self._mysql.insert_many('transfer_queue',trqnames, lambda x: x, requests)
            request_mapping[reqid] = (True, [replica])

            sql = "update requests_unified set status='queued' where reqid=" + str(reqid)
            self._mysql.query(sql)

        return request_mapping

    def schedule_reassignments(self, replicas, group, comments = '', is_test = False): #override (CopyInterface)
        # for local, copying and ownership reassignment are the same thing
        self.schedule_copies(replicas, group, comments, is_test)

    def schedule_deletion(self, replica, comments = '', is_test = False): #override (DeletionInterface)
        #here we take user wishes and transfer them, if needed" into  
        #entries for file management system (DFMS)
        request_mapping = {}

    def schedule_deletions(self, replica_list, comments = '', is_test = False): 
#override (DeletionInterface)
        request_mapping = {}
        nowTime = datetime.datetime.now()
        request_mapping = {}
        operation_id = 1

        request_id = 1
        print "--- deleting num of reps = " + str(len(replica_list))

        uninames = ['item','datatype','site','reqtype','created','updated']
        trqnames = ('reqid','file','site','status')

        for replica in replica_list:
            #print replica.dataset.demand['global_demand_rank']
            if config.read_only: continue
            if is_test : continue

            #if replica.dataset.num_files > 100:
            #    continue
            dsetObj = replica.dataset
            fileList =  dsetObj.files
            targetSite = replica.site.name

            sql = "select reqid,status from requests_unified where item='" + dsetObj.name+"'"
            sql = sql + " and site='" + targetSite + "' and reqtype='delete'"
            ids = self._mysql.query(sql)

            if len(ids) == 0:
                univals = [dsetObj.name,'dataset',targetSite,'delete',nowTime,nowTime]
                sql = self.form_sqlinsert("requests_unified",uninames,univals)
                reqid = self._mysql.query(sql)
                status = 'new'
            else:
                (reqid,status) = ids[0]


            if status == 'new':
                requests = []
                for fileObj in fileList:
                    requests.append((reqid,fileObj.fullpath(),targetSite,'new'))
                self._mysql.insert_many('deletion_queue',trqnames, lambda x: x, requests)

                sql = "update requests_unified set status='queued' where reqid=" + str(reqid)
                self._mysql.query(sql)

        request_mapping[request_id] = (True,replica_list)
        return request_mapping

    def copy_status(self, request_id): #override (CopyInterface)
        status = {}
        return status

    def deletion_status(self, request_id): #override (DeletionInterface)
        status = {}
        return status

    def get_site_list(self, sites, include = ['*'], exclude = []): #override (SiteInfoSourceInterface)
        pass
                
    def set_site_status(self, sites): #override (SiteInfoSourceInterface)
        for site in sites:
            sites[site].active = Site.STAT_READY

    def get_group_list(self, groups, filt = '*'): #override (SiteInfoSourceInterface)
        pass

    def make_replica_links(self, inventory, site_filt = '*', group_filt = '*', dataset_filt = '/*/*/*'): #override (ReplicaInfoSourceInterface)
        """
        Use blockreplicas to fetch a full list of all block replicas on the site.
        sites, groups, filt are used to limit the query.
        Objects in sites and datasets should have replica information cleared.
        """
        datasets = inventory.datasets
        sites = inventory.sites
        store = inventory.store
        groups = inventory.groups

        def listWebDir(url,mpat='<a href="([^?]\S+?)/?">(\S+?)/?<'):
            urlpath = urlopen(url)
            string = urlpath.read()
            pattern = re.compile(mpat)
            subdirs = []
            matches = pattern.findall(string) 
            for ii in range(0,len(matches)):
                if matches[ii][0] == matches[ii][1] :
                    subdirs.append(matches[ii][0])
            urlpath.close()
            return subdirs
        def getWebFile(url):
             urlpath = urlopen(url)
             rstring = urlpath.read()
             urlpath.close()
             lines = rstring.split('\n')
             return lines

        allAltSites = {}
        siteIndex = 0
        allAltDatasets = {}
        
        siteURL = {}
        siteURL['T2_US_MIT'] = 'http://t2srv0017.cmsaf.mit.edu:8070/StorageDump'
        siteURL['T3_US_MIT'] = 'http://t3serv009.mit.edu/StorageDump'
        for csite in sites:
            if csite not in siteURL:
                continue

            if csite not in allAltSites:
                siteIndex += 1
                allAltSites[csite] = AltSite(siteIndex)

            subdirs = listWebDir(siteURL[csite])
            for loadDirectory in subdirs:
                lpath = siteURL[csite] + '/' + loadDirectory
                files = listWebDir(lpath)
                for ifile in files:
                    lpath = lpath + '/' + ifile
                    lines = getWebFile(lpath)
                    #now we start processing the lines
                    #directory is a datasetsname and a blockname
                    for entry in lines:
                        if entry.startswith('d'):
                            continue
                        if not entry.endswith('.root'):
                            continue
                        fields = entry.split()
                        if len(fields) < 2:
                            continue
                        rfile,cdate = fields[-1],fields[5]
                        size = int(fields[4])

                        fields = rfile.split('/')
                        rfile = '/' + '/'.join(fields[2:])
                        bl_name = ds_name = '/' + '/'.join(fields[2:-1])
                        bl_name = AltBlock.convert2md5(bl_name)

                        altDatasetObj = None
                        if ds_name not in allAltDatasets:
                            altDatasetObj = AltDataset()
                            allAltDatasets[ds_name] = altDatasetObj
                        else:
                            altDatasetObj = allAltDatasets[ds_name]

                        altBlockObj = altDatasetObj.getKid(bl_name)
                        if altBlockObj == None:
                            altBlockObj = AltBlock()
                            altDatasetObj.addKid(bl_name,altBlockObj)
                            altBlockObj.addParent(ds_name,altDatasetObj)
                        
                        altFileObj = altBlockObj.getKid(rfile)
                        if altFileObj == None:
                            altFileObj = AltFile()
                            altBlockObj.addKid(rfile,altFileObj)
                            altFileObj.addParent(bl_name,altBlockObj)
                            
                        epoch = int(time.mktime(time.strptime(cdate, '%Y-%m-%d')))
                        altFileObj.addLocation(csite,size,epoch)

                        
        for ds_name in allAltDatasets:
            dsetObj = allAltDatasets[ds_name]
            dsetObj.updateStats()
            locations = dsetObj.locatedAt()

            dataset = block = dfile = None
            if ds_name not in datasets:
                dataset = Dataset(ds_name)
                dataset.status = Dataset.STAT_VALID
                datasets[ds_name] = dataset
            else:
                dataset = datasets[ds_name]
                dataset.replicas.clear()

            for bl_name in dsetObj.allKids():
                blockObj = dsetObj.getKid(bl_name)
                blSize = blockObj.getMaxSize()
                nFiles = blockObj.getMaxNfiles()
                block = dataset.find_block(bl_name)
                if block is None:
                    block = Block(bl_name,dataset, blSize, nFiles, False)
                    dataset.blocks.add(block)

                for rfile in blockObj.allKids():
                    dfile = dataset.find_file(rfile)
                    fileObj = blockObj.getKid(rfile)
                    fiSize = fileObj.getMaxSize()
                    if dfile is None:
                        dfile = File(rfile,block,fiSize)
                        dataset.files.add(dfile)
                    #carefull here, we are looping over all locations for the dataset
                    #have to check if the block or file actually exists here
                    for csite in locations:
                        custodial = False
                        if csite.startswith("T2_"): 
                            custodial = True
                        dataset_replica = dataset.find_replica(csite)
                        if dataset_replica is None:
                            dataset_replica = DatasetReplica(dataset,sites[csite],
                                                             dsetObj.isComplete(csite),
                                                             custodial,
                                                             dsetObj.lastUpdated(csite))
                            dataset.replicas.add(dataset_replica)
                            sites[csite].dataset_replicas.add(dataset_replica)
                        else:
                            "dataset replica already exists"
                    
                        if not blockObj.locatedAtSite(csite):
                            continue
                        block_replica = sites[csite].find_block_replica(block)
                        if block_replica == None:
                            block_replica = BlockReplica(
                                block,sites[csite],
                                groups['AnalysisOps'],
                                blockObj.isComplete(csite),
                                custodial,
                                blockObj.sizeAtSite(csite),
                                0
                            )
                            dataset_replica.block_replicas.append(block_replica)
                            sites[csite].add_block_replica(block_replica)

        print "all done here"
   
    def set_dataset_details(self, datasets, skip_valid = False): #override (DatasetInfoSourceInterface)
        """
        Argument datasets is a {name: dataset} dict.
        skip_valid is True for routine inventory update.
        """
        pass

    def _set_dataset_constituent_info(self, datasets):
        """
        Query phedex "data" interface and fill the list of blocks.
        Argument is a list of datasets.
        """
        pass

    def _set_dataset_status_and_type(self, datasets):
        """
        Use DBS 'datasetlist' to set dataset status and type.
        Called by fill_dataset_info to inquire about production/unknown datasets,
        or by set_dataset_details for a full scan.
        Argument is a list of datasets.
        """
        pass
        
