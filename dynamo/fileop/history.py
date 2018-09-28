import logging
import time

try:
    import matplotlib as mpl
    mpl.use('Agg')
    import matplotlib.pyplot as plt

except ImportError:
    class PyPlot(object):
        def hist(self, *args):
            raise RuntimeError('matplotlib is not installed on this host.')

    plt = PyPlot()

from dynamo.dataformat.fileop import Deletion, Transfer
from dynamo.history.history import HistoryDatabase

LOG = logging.getLogger(__name__)

def histogram_binning(tmin,tmax):
    nbins = int((tmax-tmin)/604800.)    # weeks
    if nbins < 10:
        nbins = int((tmax-tmin)/86400.) # days
    if nbins < 10:
        nbins = int((tmax-tmin)/3600.)  # hours
    if nbins < 10:
        nbins = int((tmax-tmin)/60.)    # minutes
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

        sql = "select t.id,f.name,site_id,exitcode,created,started,finished," + \
              " batch_id,f.size,exitcode from file_deletions as t" + \
              " inner join files as f on t.file_id=f.id" + \
              " inner join sites as s on s.id = t.site_id " + \
              condition

        LOG.info(" SQL %s"%(sql))

        start = time.time()
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
    
        elapsed = time.time() - start
        LOG.info(" processing done %s", elapsed)

        return

    def timeseries(self,graph,tmin,tmax):

        # data container
        data = []

        # derive basic characteristics
        (nbins,dt) = histogram_binning(tmin,tmax)

        # implement a dictionary with each time series
        series = {}
        for deletion in self.list:
            key = deletion.source

            # initialize a new series
            if key in series:
                pass
            else:
                series[key] = {'times': [], 'y_values': []}
            
            serie = series[key]
            serie['times'].append(deletion.end)

            if graph[0] == 'n': # just the number of deletions -> no weights (size)
                serie['y_values'].append(1)                               # number of deletions                
            else:
                serie['y_values'].append(deletion.size/1000./1000./1000.) # in GB

        # loop through all different requested time series
        total_hist = []
        for key in series:

            # get the time serie
            serie = series[key]
            # use matplotlib to extract histogram information
            hist,bins,p = plt.hist(serie['times'],nbins,range=(tmin,tmax),weights=serie['y_values'])

            # now generate the serializable object
            name = key
            cs = 0
            datum = { 'name': name, 'data': [] }
            i = 0
            for t,y in zip(bins,hist):

                yval = y
                if   graph[0] == 'c':         # cumulative volume
                    cs += y
                    yval = cs
                elif graph[0] == 'r':         # rate (volume per time)
                    yval = y/dt
                datum['data'].append({'time': t, 'y_value': yval })
                # make sure to keep our histogram up to speed for later use
                hist[i] = yval
                i += 1

            # keep track of the sum of all historgrams
            if total_hist == []:
                total_hist = hist
            else:
                i = 0
                for value in hist:
                    total_hist[i] += value
                    i += 1

            # append the full site information
            data.append(datum)

        # make sure if the data array is empty to add an empty dictionary
        if len(data) < 1:
            data.append({})

        # calculate summary
        min_value = 0
        max_value = 0
        avg_value = 0
        cur_value = 0
        if len(total_hist) > 1:  # careful Bytes -> GBytes
            min_value = min(total_hist)
            max_value = max(total_hist)
            avg_value = sum(total_hist)/len(total_hist)
            cur_value = total_hist[-1]

        return (min_value,max_value,avg_value,cur_value,data)

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

        # implement a dictionary with each time series
        series = {}
        for transfer in self.list:

            if entity == 'dest':
               key = transfer.target
            elif entity == 'src':
               key = transfer.source
            else:
               key = "%s->%s"%(transfer.source,transfer.target)

            if key in series:
                pass
            else:
                series[key] = {'times': [], 'y_values': []}
            
            serie = series[key]
            serie['times'].append(transfer.end)

            if graph[0] == 'n': # just the number of transfers -> no weights (size)
                serie['y_values'].append(1)                               # number of transfers                
            else:
                serie['y_values'].append(transfer.size/1000./1000./1000.) # in GB

        # loop through all different requested time series
        total_hist = []
        for key in series:

            # get the time serie
            serie = series[key]
            # use matplotlib to extract histogram information
            hist,bins,p = plt.hist(serie['times'],nbins,range=(tmin,tmax),weights=serie['y_values'])

            # now generate the serializable object
            name = key
            cs = 0
            datum = { 'name': name, 'data': [] }
            i = 0
            for t,y in zip(bins,hist):

                yval = y
                if   graph[0] == 'c':         # cumulative volume
                    cs += y
                    yval = cs
                elif graph[0] == 'r':         # rate (volume per time)
                    yval = y/dt
                datum['data'].append({'time': t, 'y_value': yval })
                # make sure to keep our histogram up to speed for later use
                hist[i] = yval
                i += 1

            # keep track of the sum of all historgrams
            if total_hist == []:
                total_hist = hist
            else:
                i = 0
                for value in hist:
                    total_hist[i] += value
                    i += 1

            # append the full site information
            data.append(datum)

        # make sure if the data array is empty to add an empty dictionary
        if len(data) < 1:
            data.append({})

        # calculate summary
        min_value = 0
        max_value = 0
        avg_value = 0
        cur_value = 0
        if len(total_hist) > 1:  # careful Bytes -> GBytes
            min_value = min(total_hist)
            max_value = max(total_hist)
            avg_value = sum(total_hist)/len(total_hist)
            cur_value = total_hist[-1]

        return (min_value,max_value,avg_value,cur_value,data)
