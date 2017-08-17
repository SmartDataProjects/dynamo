import time
import datetime
import collections
import fnmatch
import logging

from common.interface.mysql import MySQL
from common.dataformat import Dataset, DatasetReplica, Block, BlockReplica, Site, Group
import common.configuration as config

logger = logging.getLogger(__name__)

class UserRequest:
    def __init__(self,reqid,name,siteTo,reqType,created):
        self._reqid = reqid
        self._itemName = name
        self._siteTo = siteTo
        self._reqType = reqType
        self._created = created
        self._files = {}

    def markDone(self,fileName,fileObj):
        if '/' in fileName:
            fileName = fileName.split('/')[-1]
        if fileName in self._files:
            self._files[fileName] = fileObj
        else:
            raise BaseException(fileName + "should not be in " + self._itemName )

    def getSize(self,block):
        size = 0
        for fileName in self._files:
            if self._files[fileName] == 0:
                continue
            fileObj = self._files[fileName]
            if fileObj.block == block:
                size += fileObj.size 
        return size

class QueueManager(object):

    def __init__(self, inventory, history):
        self._inventory = inventory
        self._history = history
        self._mysql = MySQL(**config.mysqlregistry.db_params)

    def getTransfers(self,requests):
        sql = "select tq.reqid,tq.file,tq.site_from,tq.site_to,tq.status"
        sql = sql + " from transfer_queue as tq,requests_unified as ru"
        sql = sql + " where tq.status='done' and tq.reqid=0"
        entries = self._mysql.query(sql)
        singlesToDelete = []
        for line in entries:
            reqid = int(line[0])
            (fileName,siteFrom,siteTo,status) = line[1:]
            if reqid == 0 :
                singlesToDelete.append((fileName,siteTo))

        self._mysql.delete_many('transfer_queue',('file','site_to'),singlesToDelete)

        sql = "select tq.reqid,tq.file,tq.site_from,tq.site_to,tq.status"
        sql = sql + " from transfer_queue as tq,requests_unified as ru"
        sql = sql + " where tq.status='done' and tq.reqid=ru.reqid"
        entries = self._mysql.query(sql)
        for line in entries:
            reqid = int(line[0])
            (fileName,siteFrom,siteTo,status) = line[1:]
            if reqid == 0 :
                continue

            stripedName = (fileName.split('/'))[-1]
            uRequest = requests[reqid]
            dsetObj = self._inventory.datasets[uRequest._itemName]
            fileObj = None
            for filef in dsetObj.files:
                if filef.name == stripedName:
                    fileObj = filef
                    break
            uRequest.markDone(stripedName,fileObj)

    def getDeletions(self,requests):
        sql = "select dq.reqid,dq.file,dq.site,dq.status"
        sql = sql + " from deletion_queue as dq where dq.status='done' and dq.reqid=0"
        entries = self._mysql.query(sql)
        singlesToDelete = []
        for line in entries:
            reqid = int(line[0])
            (fileName,site,status) = line[1:]
            if reqid == 0 :
                singlesToDelete.append((fileName,site))
        self._mysql.delete_many('deletion_queue',('file','site'),singlesToDelete)


        sql = "select dq.reqid,dq.file,dq.site,dq.status"
        sql = sql + " from deletion_queue as dq,requests_unified as ru"
        sql = sql + " where dq.status='done' and dq.reqid=ru.reqid"
        entries = self._mysql.query(sql)
        print '------------'
        for line in entries:
            reqid = int(line[0])
            (fileName,site,status) = line[1:]
            if reqid == 0 :
                continue
            
            stripedName = (fileName.split('/'))[-1]
            uRequest = requests[reqid]
            dsetObj = self._inventory.datasets[uRequest._itemName]
            fileObj = None
            for filef in dsetObj.files:
                if filef.name == stripedName:
                    fileObj = filef
                    break
            uRequest.markDone(stripedName,fileObj)

    def fillDoneTransfers(self,requests):
        #for finished requests we update history first, then delete
        #for unfinished requests we update status and timestamps
        new_dataset_replicas = []
        replica_timestamps = {}
        done_requests = []
        for reqid in sorted(requests):
            uRequest = requests[reqid]
            dataset = self._inventory.datasets[uRequest._itemName]
            site    = self._inventory.sites[uRequest._siteTo]
            reqtype = uRequest._reqType
            if reqtype != 'copy':
                continue
                
            dsetRep = dataset.find_replica(site)
            #new block replica is derived from existing replica
            targetGroup = None
            for someRep in dataset.replicas:
                if someRep == dsetRep:
                    continue
                else:
                    targetGroup = someRep.block_replicas[0].group
                    break

            #ask for size, update only if size is changing
            if dsetRep == None:
                dsetRep = self._inventory.add_dataset_to_site(dataset,site,targetGroup)
                print "making new dataset replica"

            dsetDone = True
            for blockRep in dsetRep.block_replicas:
                block = blockRep.block
                size = uRequest.getSize(block)
                complete = False
                if size == block.size:
                    complete = True
                else:
                    dsetDone = False
                    
                if size > blockRep.size:
                    print 'updating block replica ...'
                    dsetRep.update_block_replica(block, targetGroup, complete, False, size)
                    

            #here we enter done requests into the history databas
            #and delete them them from ongoing activities
            print dsetRep.dataset.name
            print "dset done status = " + str(dsetDone)
            if 0 in uRequest._files.itervalues():
                print "reqid=" + str(reqid) + " request not finished"
            else:
                print "reqid=" + str(reqid) + " request is done !!!"
                new_dataset_replicas.append(dsetRep)
                replica_timestamps[dsetRep] = uRequest._created
                done_requests.append(uRequest._reqid)
                dsetRep.is_complete = True

        #save complete requests into history
        self._history.save_dataset_transfers(new_dataset_replicas,replica_timestamps)
        #and delete from registry
        self._mysql.delete_many('requests_unified','reqid',done_requests)
        self._mysql.delete_many('transfer_queue','reqid',done_requests)

    def fillDoneDeletions(self,requests):
        #for finished requests we update history first, then delete
        #for unfinished requests we update status and timestamps
        gone_dataset_replicas = []
        replica_timestamps = {}
        done_requests = []
        for reqid in sorted(requests):
            uRequest = requests[reqid]
            dataset = self._inventory.datasets[uRequest._itemName]
            site    = self._inventory.sites[uRequest._siteTo]
            reqtype = uRequest._reqType
            if reqtype != 'delete':
                continue
            
            print '------------'
            print reqid

            dsetRep = dataset.find_replica(site)

            #deleting something that does not exist
            if dsetRep == None:
                print "..!!.. trying to delete non-existing dataset"
                print site.name
                print dataset.name
                continue

            #here we enter done requests into the history databas
            #and delete them them from ongoing activities
            print dsetRep.dataset.name
            if 0 in uRequest._files.itervalues():
                print "reqid=" + str(reqid) + " request not finished"
            else:
                print "reqid=" + str(reqid) + " request is done !!!"
                gone_dataset_replicas.append(dsetRep)
                replica_timestamps[dsetRep] = uRequest._created
                done_requests.append(uRequest._reqid)
                print dsetRep.block_replicas
                self._inventory.unlink_datasetreplica(dsetRep)

        #save complete requests into history
        self._history.save_dataset_deletions(gone_dataset_replicas,replica_timestamps)
        #and delete from registry
        self._mysql.delete_many('requests_unified','reqid',done_requests)
        self._mysql.delete_many('deletion_queue','reqid',done_requests)


    def run(self, comment = ''):
        requests = {}
        logger.info('QueueManager run starting at %s', time.strftime('%Y-%m-%d %H:%M:%S'))

        sql = "select * from requests_unified where status='queued'"
        entries = self._mysql.query(sql)
        for line in entries:
            reqid = int(line[0])
            (itemName,datatype,siteTo,reqtype,rank,status,created,updated) = line[1:]
            dsetObj = self._inventory.datasets[itemName]
            requests[reqid] = UserRequest(reqid,itemName,siteTo,reqtype,created)
            for fileObj in dsetObj.files:
                requests[reqid]._files[fileObj.name] = 0

        self.getTransfers(requests)
        self.getDeletions(requests)

        self.fillDoneTransfers(requests)
        self.fillDoneDeletions(requests)

        logger.info('Finished QueueManager run at %s\n', time.strftime('%Y-%m-%d %H:%M:%S'))
        
