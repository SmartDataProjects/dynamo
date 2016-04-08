from common.configuration import paths, Configuration

delete_old = Configuration()
delete_old.threshold = (1.5, 'y')

delete_unpopular = Configuration()
delete_unpopular.threshold = 1.

#log_path = paths.log_directory + '/detox_policy_log.txt'
log_path = '/local/yiiyama/deletion/detox_policy_log.txt'
html_path = '/var/www/html/detox/policylog.html'
