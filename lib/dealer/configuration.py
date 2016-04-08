from common.configuration import Configuration

operating_group = 'AnalysisOps'

demand_refresh_interval = 7200. # update demand if demand manager time_today is more than 7200 seconds ago

occupancy_fraction_threshold = 0.015 # (CPU hour) / (time normalisation (s)) / (site CPU capacity (kHS06))

max_replicas = 10

summary_html = '/var/www/html/dealer/copy_decisions.html'

