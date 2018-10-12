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
        exit_code = '0'

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
        if 'exit_code' in request:
            exit_code = request['exit_code']

        # calculate the time limits to consider
        past_min = self._get_date_before_end(datetime.datetime.now(),upto)
        tmax = int(past_min.strftime('%s')) # epochseconds: careful max/min in t and past inverts
        past_max = self._get_date_before_end(past_min,period)
        tmin = int(past_max.strftime('%s')) # epochseconds

        # get our transfer data once
        start = time.time()
        transfers = Transfers()
        filter_string =  " where finished >= '%s' and finished < '%s'"%(past_max,past_min) + \
            self._add_filter_conditions(entity,dest_filter,src_filter,no_mss,exit_code)
        transfers.read_db(condition = filter_string)
        elapsed_db = time.time() - start
        LOG.info('Reading transfers from db: %7.3f sec', elapsed_db)

        # parse and extract the plotting data (timeseries guarantees an empty dictionary as data)
        start = time.time()
        (min_value,max_value,avg_value,cur_value,data) = \
            transfers.timeseries(graph,entity,tmin,tmax)
        elapsed_processing = time.time() - start
        LOG.info('Parsed data: %7.3f sec', elapsed_processing)
        
        # find bin width and time unit
        (delta_t,dt,unit) = self._time_constants(tmin,tmax,len(data[0]))

        # generate summary string and yaxis_label
        yaxis_label = 'Transfered Volume [GB]'
        summary_string = "Min: %.3f GB, Max: %.3f GB, Avg: %.3f GB, Last: %.3f GB" \
            %(min_value,max_value,avg_value,cur_value)

        if     graph[0] == 'r':         # cumulative volume
            yaxis_label = 'Transfered Rate [GB/sec]'
            summary_string = "Min: %.3f GB/s, Max: %.3f GB/s, Avg: %.3f GB/s, Last: %.3f GB/s" \
                %(min_value,max_value,avg_value,cur_value)
        elif   graph[0] == 'c':         # cumulative volume
            yaxis_label = 'Cumulative Transfered Volume [GB]'
            summary_string = "Total: %.3f GB, Avg Rate: %.3f GB/s"%(cur_value,cur_value/delta_t)
        elif   graph[0] == 'n':         # number of transfers
            yaxis_label = 'Number of Transfers'
            summary_string = "Min: %.0f, Max: %.0f, Avg: %.0f, Last: %.0f" \
                %(min_value,max_value,avg_value,cur_value)
        yaxis_label += unit

        # add text graphics information to the plot
        data[0]['yaxis_label'] = yaxis_label
        data[0]['title'] = 'Dynamo Transfers (%s by %s)'%(graph,entity)
        data[0]['subtitle'] = 'Time period: %s -- %s'%(str(past_max).split('.')[0],str(past_min).split('.')[0])
        data[0]['timing_string'] = \
            'db:%.2fs, processing:%.2fs'%(elapsed_db,elapsed_processing)
        data[0]['summary_string'] = summary_string

        return data

    def _get_date_before_end(self,end,before):
        # looking backward in time: find date a period of 'before' before the date 'end'
        #  before ex. 10h, 5d, 10w

        past_date = end
        
        u = before[-1]
        n = int(before[:-1])

        if   u == 'd':
            n = n*24
            past_date -= datetime.timedelta(hours=n)
        elif u == 'h':
            past_date -= datetime.timedelta(hours=n)
        elif u == 'w':
            n = n*24*7
            past_date -= datetime.timedelta(hours=n)
        else:
            LOG.error('no properly defined unit (d- days, h- hours, w- weeks): %s'%(before))
                      
        return past_date

    def _add_filter_conditions(self,entity,dest_filter,src_filter,no_mss,exit_code):

        # default string is empty (doing nothing)
        filter_string = ""

        # is there any filtering at all
        if src_filter != "":
            filter_string += " and s.name like '%s'"%(src_filter)
        if dest_filter != "":
            filter_string += " and d.name like '%s'"%(dest_filter)

        if no_mss:
            filter_string += " and s.name not like '%%MSS' and d.name not like '%%MSS'"

        if   exit_code[0] == "*":
            pass                  # no further filtering
        elif exit_code[0] == "!":
            filter_string += " and exitcode != %s"%(exit_code[1:])
        else:
            filter_string += " and exitcode = %s"%(exit_code)

        return filter_string

    def _time_constants(self,tmin,tmax,nbins):

        unit = ''
        dt = -1

        delta_t = tmax-tmin
        if nbins>0:
            dt = delta_t/nbins

        if      abs(dt-604800) < 1:
            unit = ' / week';
        elif abs(dt-86400.) < 1:
            unit = ' / day';
        elif abs(dt-3600.) < 1:
            unit = ' / hour';
        elif abs(dt-60) < 1:
            unit = ' / minute';

        return (delta_t,dt,unit)

export_data = {
    'history': FileTransferHistory
}
