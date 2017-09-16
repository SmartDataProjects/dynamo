from common.dataformat import Configuration

main = Configuration(
    activity_indicator = '/home/cmsprod/public_html/IntelROCCS/Detox/inActionLock.txt',
    deletion_per_iteration = 0.01, # fraction of quota to delete per iteration
    deletion_volume_per_request = 50, # size to delete per deletion request in TB
    time_shift = 0. # number of days in the future from which to evaluate the policies
)
