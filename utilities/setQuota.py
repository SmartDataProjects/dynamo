from common.interface.mysql import MySQL

"""
Usage examples:
Both:
/usr/bin/python setQuota.py --easy_scale 0.4 --site "T2_US_MIT"
Absolute:
/usr/bin/python setQuota.py --volume 999 --site "T2_US_MIT" --partition DataOps (--adjust_other)
Relative:
/usr/bin/python setQuota.py --volume 0.4 --site "T2_US_MIT" --partition DataOps 
"""

# Define database and dictionaries
dynamo = MySQL(config_file = '/etc/my.cnf', config_group = 'mysql-dynamo', db = 'dynamo')
sites = dynamo.xquery('SELECT s.`id`, s.`name`, q.`storage`, q.`partition_id` FROM `sites` AS s INNER JOIN `quotas` AS q ON q.`site_id` = s.`id`')
sitenames = []
siteids = {}
main_partitions = ["AnalysisOps","DataOps"]
main_partitions_dicts = {}
dict_anOps = {}
dict_daOps = {}
main_partitions_dicts["AnalysisOps"] = dict_anOps
main_partitions_dicts["DataOps"] = dict_daOps

# Fill all partition sizes
for ide, name, storage, partition in sites:
    if name not in sitenames:
        sitenames.append(name)
        siteids[name] = ide
    if partition == 1:
        dict_anOps[name] = storage
    if partition == 2:
        dict_daOps[name] = storage

def yes_or_no(question):
    reply = str(raw_input(question+' (y/n): ')).lower().strip()
    if reply[0] == 'y':
        return True
    if reply[0] == 'n':
        return False
    else:
        return yes_or_no("Uhhhh... please enter")

def change_quota(site,projected_quota):
    fields = ('site_id', 'partition_id', 'storage')
    
    data = []
    for partition, value in projected_quota.iteritems():
        data.append((siteids[site],1 if partition == "AnalysisOps" else 2, projected_quota[partition]))

    dynamo.insert_many('quotas', fields, None, data, do_update = True)

if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(description = 'Use this script to change the quota of site partitions.')

    parser.add_argument('--site', '-s', metavar = 'SITE', dest = 'site', default = 'T2_US_MIT', help = 'Site name.')
    parser.add_argument('--partition', '-g', metavar = 'PARTITION', dest = 'partition', default = 'AnalysisOps', help = 'Partition name.')
    parser.add_argument('--volume', '-v', metavar = 'VOLUME', dest = 'volume', help = 'Size of partition in TB. To specify a fraction, use a number <=1.0')
    parser.add_argument('--adjust_other', '-a', action = 'store_true', dest = 'adjust_other', help = 'Automatically adjust other partition to keep sum the same? Default: False')
    parser.add_argument('--easy_scale', '-e',  dest = 'easy_scale', help = 'Automatically scale both partitions by factor')
    parser.add_argument('--dump', '-d', action = 'store_true', dest = 'dump', help = 'Just print all of them. Default: False')
    
    args = parser.parse_args()

    current_quota = {}
    partition_sum = 0

    if args.site not in sitenames:
        print "Not a valid sitename"
        exit(1)

    # Giving an overview
    print "\nCurrent quota "

    for partition, dic in main_partitions_dicts.iteritems():
        for site, value in dic.iteritems():
            if site != args.site and not args.dump:
                continue
            else:                
                current_quota[partition] = value
                partition_sum += value if site == args.site else 0
                print "Site %s | Partition %s | Quota %i TB" % (site, partition, value)         

    # Calculating projected quotas
    projected_quota = {}
    if args.volume:
        projected_quota[args.partition] = int(args.volume) if not "." in args.volume else int(float(args.volume)*partition_sum)

        if args.partition == "AnalysisOps":
            projected_quota["DataOps"] = partition_sum - projected_quota["AnalysisOps"] if args.adjust_other or float(args.volume)<=1.0 and "." in args.volume else current_quota["DataOps"]
        else:
            projected_quota["AnalysisOps"] = partition_sum - projected_quota["DataOps"] if args.adjust_other or float(args.volume)<=1.0 and "." in args.volume else current_quota["AnalysisOps"]

    if args.easy_scale:
        projected_quota["DataOps"] = current_quota["DataOps"]*float(args.easy_scale)
        projected_quota["AnalysisOps"] = current_quota["AnalysisOps"]*float(args.easy_scale)

    # Communicate
    print "\nProjected quota"

    for partition, value in projected_quota.iteritems():        
        print "Site %s | Partition %s | Quota %i TB" % (args.site, partition, value)         

    if yes_or_no("Is this correct?"):
        change_quota(args.site, projected_quota)

    print "Done."
