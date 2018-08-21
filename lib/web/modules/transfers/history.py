import time
import datetime
import logging

from dynamo.web.modules._base import WebModule
from dynamo.history.history import HistoryDatabase

from dynamo.fileop.history import Transfers

LOG = logging.getLogger(__name__)

class FileTransferHistory(WebModule):
    def __init__(self, config):
        WebModule.__init__(self, config)

        self.history = HistoryDatabase()

    def run(self, caller, request, inventory):

        # give all the request dictionary
        LOG.debug(str(request))

        # defaults
        graph = 'volume'
        entity = 'dest'
        src_filter = ''
        dest_filter = ''
        no_mss = True
        period = '24h'
        upto = '0h'

        # reading the requests
        if 'graph' in request:
            graph = request['graph']
        if 'entity' in request:
            entity = request['entity']
        if 'src_filter' in request:
            src_filter = request['src_filter'].replace("%","%%")
        if 'dest_filter' in request:
            dest_filter = request['dest_filter'].replace("%","%%")
        if 'no_mss' in request:
            if request['no_mss'] == 'false':
                no_mss = False
        if 'period' in request:
            period = request['period']
        if 'upto' in request:
            upto = request['upto']

        # calculate the time limits to consider
        past_min = self._get_date_before_end(datetime.datetime.now(),upto)
        past_max = self._get_date_before_end(past_min,period)

        # get our transfer data once
        start = time.time()
        transfers = Transfers()
        filter_string =  " where finished >= '%s' and finished < '%s'"%(past_max,past_min) + \
            self._add_filter_conditions(entity,dest_filter,src_filter,no_mss)
        transfers.read_db(condition = filter_string)
        elapsed_db = time.time() - start
        LOG.info('Reading transfers from db: %7.3f sec', elapsed_db)

        # parse and extract the plotting data
        start = time.time()
        data = transfers.timeseries(graph,entity,int(past_max.strftime('%s')),int(past_min.strftime('%s')))
        elapsed_processing = time.time() - start
        LOG.info('Parsed data: %7.3f sec', elapsed_processing)

        # add timing information to the plot
        if len(data) < 1:
            data.append({})
        data[0]['title'] = \
            'Dynamo Transfers (%s by %s)'%(graph,entity)
        data[0]['subtitle'] = \
            'Time period: %s -- %s'%(str(past_max).split('.')[0],str(past_min).split('.')[0])
        data[0]['timing_string'] = \
            'Timing -- db: %.3f sec, processing: %.3f sec'%(elapsed_db,elapsed_processing)

        return data

    def _get_date_before_end(self,end,before):
        # looking backward in time: find date a period of 'before' before the date 'end'
        #  before ex. 10h, 5d, 10w

        past_date = end
        
        u = before[-1]
        n = int(before[:-1])

        if   u == 'd':
            past_date -= datetime.timedelta(days=n)
        elif u == 'h':
            past_date -= datetime.timedelta(hours=n)
        elif u == 'w':
            past_date -= datetime.timedelta(weeks=n)
        else:
            LOG.error('no properly defined unit (d- days, h- hours, w- weeks): %s'%(before))
                      
        return past_date

    def _add_filter_conditions(self,entity,dest_filter,src_filter,no_mss):

        # default string is empty (doing nothing)
        filter_string = ""

        # is there any filtering at all
        if src_filter != "":
            filter_string += " and s.name like '%s'"%(src_filter)
        if dest_filter != "":
            filter_string += " and d.name like '%s'"%(dest_filter)

        if no_mss:
            filter_string += " and s.name not like '%%MSS' and d.name not like '%%MSS'"

        return filter_string

export_data = {
    'history': FileTransferHistory
}
