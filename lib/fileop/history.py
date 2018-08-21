import logging
import time

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

from dynamo.dataformat.fileop import Deletion, Transfer
from dynamo.history.history import HistoryDatabase

LOG = logging.getLogger(__name__)

def histogram_binning(tmin,tmax):
    nbins = int((tmax-tmin)/84600.)
    if nbins < 10:
        nbins = int((tmax-tmin)/3600.)
    dt = (tmax-tmin)/nbins

    return (nbins,dt)

class Sites:
    """
    Defines the sites.
    """
    def __init__(self):
        self.names = [0] * 500  # bad hack but whatever

    def read_db(self,history_db):
        sql = "select id,name from sites"
        results = history_db.db.query(sql)
        for row in results:
            self.names[int(row[0])] = row[1]
        return

    def find_id(self,site_name):
        id = -1
        i = 0
        for name in self.names:
            if name == site_name:
                break
            i += 1
        return i

class Operations:
    """
    Glorified container class to manage a bunch of data operations. This is the base class.
    """

    def __init__(self):

        self.list = []         # list of all operations
        self.n_sources = {}    # dictionary n operations per sources
        self.time_bins = []
        self.volume_bins = []
        self.history_db = HistoryDatabase()   # usage: result = self.history_db.db.query('SELECT ...')
        self.sites = Sites()   # we need to have a translation from site_id to site_name
        self.sites.read_db(self.history_db)   # - get the current list of all sites by id

class Deletions(Operations):
    """
    Container class to manage a bunch of deletions.
    """

    def __init__(self):
        Operations.__init__(self)
        
    def read_db(self,condition=""):

        sql = "select t.id,f.name,site_id,exitcode," \
            + " created,started,finished,batch_id,f.size,exitcode " \
            + " from file_deletions as t inner join files as f on t.file_id=f.id" \
            + condition
        results = self.history_db.db.query(sql)

        for row in results:
            deletion = Deletion()
            deletion.from_row(row,self.sites)
            if deletion.size > -1:
                self.list.append(deletion)
                if deletion.source in self.n_sources:
                    self.n_sources[deletion.source] += 1
                else:
                    self.n_sources[deletion.source] = 1
    
        return

class Transfers(Operations):
    """
    Defines a bunch of unique transfers.
    """

    def __init__(self):
        Operations.__init__(self)
        self.n_targets = {}  # dictionary n transfers per targets
        
    def read_db(self,condition=""):
        sql = "select t.id,f.name,source_id,destination_id,exitcode,created,started,finished," + \
              " batch_id,f.size,exitcode from file_transfers as t" + \
              " inner join files as f on t.file_id=f.id" + \
              " inner join sites as d on d.id = t.destination_id" + \
              " inner join sites as s on s.id = t.source_id " + \
              condition
        LOG.info(" SQL %s"%(sql))
        start = time.time()
        results = self.history_db.db.query(sql)
        for row in results:
            transfer = Transfer()
            transfer.from_row(row,self.sites)

            if transfer.size > -1:

                self.list.append(transfer)
                LOG.debug(" Append %s"%(str(transfer)))

                if transfer.source in self.n_sources:
                    self.n_sources[transfer.source] += 1
                else:
                    self.n_sources[transfer.source] = 1

                if transfer.target in self.n_targets:
                    self.n_targets[transfer.target] += 1
                else:
                    self.n_targets[transfer.target] = 1

        elapsed = time.time() - start
        LOG.info(" processing done %s", elapsed)

        return

    def timeseries(self,graph,entity,tmin,tmax):

        # data container
        data = []

        # derive basic characteristics
        (nbins,dt) = histogram_binning(tmin,tmax)

        # sites to order by
        n_sites = {}
        if entity == 'dest' or entity == 'link':
            n_sites = self.n_targets
        elif entity == 'src':
            n_sites = self.n_sources

        # loop through the list of relevant sites separately
        for site in n_sites:

            # what should we expect
            if entity == 'src':
                LOG.info(" Source: %s,  n_transfers: %d"%(site,n_sites[site]))
            else:
                LOG.info(" Target: %s,  n_transfers: %d"%(site,n_sites[site]))

            # initialize
            times = []
            sizes = []
   
            # get the times and sizes of the transfers
            for transfer in self.list:
                if ((entity == 'dest' or entity == 'link') and transfer.target == site) or \
                   (entity == 'src' and transfer.source == site):
                    size = transfer.size
                    times.append(transfer.end)
                    sizes.append(transfer.size)
    

            # use matplotlib to extract histogram information
            hist,bins,p = plt.hist(times,nbins,range=(tmin,tmax),weights=sizes)

            # now generate the serializable object
            name = site
            cs = 0
            datum = { 'name': name, 'data': [] }
            for t,s in zip(bins,hist):

                size = s
                if   graph[0] == 'c':         # cumulative volume
                    cs += s
                    size = cs
                elif graph[0] == 'r':         # rate (volume per time)
                    size = s/dt
                datum['data'].append({'time': t, 'size': size })

            # append the full site information
            data.append(datum)

        return data
