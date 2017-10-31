from common.dataformat import Configuration

main = Configuration(
    target_sites = ['T2_*', 'T1_*_Disk', '!T2_GR_Ioannina', '!T2_TR_METU'],
    max_dataset_size = 50., # Maximum dataset size to consider for copy in TB
    max_copy_per_site = 50., # Maximum volume to be copied per site in TB
    max_copy_total = 200.,
    source_groups = ['AnalysisOps'],
    target_site_occupancy = 0.9,
    skip_existing = True
)

popularity = Configuration(
    request_to_replica_threshold = 1.75, # (weighted number of requests) / (number of replicas) above which replication happens
    max_replicas = 10
)

# balancer considers dataset replicas protected for the following reasons
# the number is the minimum number of non-partial replicas above which balancer ignores the dataset
balancer = Configuration(
    target_reasons = [
        ('dataset.name == /*/*/MINIAOD* and replica.num_full_disk_copy_common_owner < 3', 3),
        ('replica.num_full_disk_copy_common_owner < 2', 2)
    ]
)
