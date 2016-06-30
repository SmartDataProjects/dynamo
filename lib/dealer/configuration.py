import common.configuration as common

demand_refresh_interval = 7200. # update demand if demand manager time_today is more than 7200 seconds ago

max_dataset_size = 10. # Maximum dataset size to consider for copy in TB

request_to_replica_threshold = 1.75 # (weighted number of requests) / (number of replicas) above which replication happens

max_copy_per_site = 6. # Maximum volume to be copied per site in TB
max_copy_total = 100.

max_replicas = 10

overflow_factor = 1.05 # Potentially copy up to target occupancy * overflow_factor

excluded_destinations = [
    'T1_DE_KIT_Disk',
    'T1_ES_PIC_Disk',
    'T1_FR_CCIN2P3_Disk',
    'T1_IT_CNAF_Disk',
    'T1_RU_JINR_Disk',
    'T1_UK_RAL_Disk',
    'T1_US_FNAL_Disk',
    'T2_EE_Estonia',
    'T2_GR_Ioannina',
    'T2_IN_TIFR',
    'T2_PK_NCP',
    'T2_PL_Warsaw',
    'T2_RU_INR',
    'T2_RU_SINP',
    'T2_RU_PNPI',
    'T2_RU_RRC_KI',
    'T2_TH_CUNSTDA',
    'T2_RU_ITEP',
    'T2_TR_METU',
    'T2_US_Vanderbilt'
]

summary_html = '/home/cmsprod/public_html/dynamo/dealer/copy_decisions.html'
