from common.configuration import paths, Configuration

keep_target = Configuration()
keep_target.occupancy = 0.85

delete_old = Configuration()
delete_old.threshold = (1.5, 'y')

log_path = paths.log_directory + '/detox_policy_log.txt'
