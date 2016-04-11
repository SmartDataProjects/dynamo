from common.configuration import Configuration

operating_group = 'AnalysisOps'

demand_refresh_interval = 7200. # update demand if demand manager time_today is more than 7200 seconds ago

occupancy_fraction_threshold = 0.1 # (CPU hour) / (time normalisation (s)) / (site CPU capacity (kHS06))

max_copy_volume = 6. # Maximum volume to be copied per site in TB

max_replicas = 5

summary_html = '/var/www/html/dealer/copy_decisions.html'

