import sys
import pickle
import logging
import collections
from dynamo.core.executable import inventory, make_standard_logger
from dynamo.dataformat import Configuration, Block, Group
from dynamo.source.impl.phedexdatasetinfo import PhEDExDatasetInfoSource

LOG = make_standard_logger('info')

site = inventory.sites['T2_AT_Vienna']
#dataset = inventory.datasets['/SingleMuon/Run2016G-07Aug17-v1/AOD']
dataset = inventory.datasets['/BBbarDMJets_scalar_Mchi-50_Mphi-95_TuneCUETP8M1_v2_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM']
dataset_replica = site.find_dataset_replica(dataset)
physics = inventory.partitions['Physics']
unsubscribed = inventory.partitions['Unsubscribed']

replica = list(dataset_replica.block_replicas)[0]

#print physics.contains(block_replica)
#print unsubscribed.contains(block_replica)
#print replica in site.partitions[physics].replicas
#print replica in site.partitions[unsubscribed].replicas
#print block_replica.group
#
#block_replica.embed_into(inventory)

if dataset_replica in site.partitions[physics].replicas:
    print 'dataset in physics'
#    print replica in site.partitions[physics].replicas[dataset_replica]
if dataset_replica in site.partitions[unsubscribed].replicas:
    print 'dataset in unsub'
#    print replica in site.partitions[unsubscribed].replicas[dataset_replica]

replica.group = Group.null_group
site.update_partitioning(replica)

#for partition in [physics, unsubscribed]:
#    print partition
#    site_partition = site.partitions[partition]
#
#    try:
#        block_replicas = site_partition.replicas[dataset_replica]
#        print 'found replicas'
#    except KeyError:
#        block_replicas = set()
#        print 'creating new list'
#
#    if partition.contains(replica):
#        print 'contains'
#        if block_replicas is None or replica in block_replicas:
#            print 'already included'
#            # already included
#            continue
#        else:
#            print 'adding'
#            block_replicas.add(replica)
#    else:
#        if block_replicas is None:
#            print 'was full, now not'
#            # this dataset replica used to be fully included but now it's not
#            # make a copy of the full list of block replicas
#            block_replicas = set(dataset_replica.block_replicas)
#            block_replicas.remove(replica)
#        else:
#            try:
#                block_replicas.remove(replica)
#                print 'removed'
#            except KeyError:
#                # not included already
#                print 'not included already'
#                pass
#
#    if len(block_replicas) == 0:
#        print 'empty'
#        try:
#            site_partition.replicas.pop(dataset_replica)
#            print 'popped'
#        except KeyError:
#            print 'wasnt there'
#            pass
#
#    elif block_replicas == dataset_replica.block_replicas:
#        print 'setting to None'
#        site_partition.replicas[dataset_replica] = None
#    else:
#        print 'setting list'
#        site_partition.replicas[dataset_replica] = block_replicas

if dataset_replica in site.partitions[physics].replicas:
    print 'dataset in physics'
    print replica in site.partitions[physics].replicas[dataset_replica]
if dataset_replica in site.partitions[unsubscribed].replicas:
    print 'dataset in unsub'
    print replica in site.partitions[unsubscribed].replicas[dataset_replica]

#for replica in site_partition.replicas.keys():
#    if replica.dataset is dataset:
#        print 'In Physics'
#
#        owners = collections.defaultdict(int)
#        for block_replica in replica.block_replicas:
#            owners[block_replica.group] += 1
#        
#        print owners
#
#        break

#partition = inventory.partitions['Physics']
#site_partition = site.partitions[partition]
#
#print (site_partition.occupancy_fraction(physical = False) - site_partition.occupancy_fraction(physical = True)) * site_partition.quota
#print site_partition.occupancy_fraction(physical = False) * site_partition.quota

