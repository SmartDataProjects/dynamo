#!/usr/bin/env python
import time
import datetime
import random
import pandas as pd
import re
import fnmatch

from dynamo.dataformat import Configuration, Block, Site
from dynamo.utils.interface.mysql import MySQL
from dynamo.core.executable import inventory

# # # # # # # #
# Query part  #
# # # # # # # #

nowtime = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')

# Dynamo history database: exitcode
dynamoregistry = MySQL(Configuration(db = 'dynamoregister', user = 'dummyuser', passwd = 'dummypassword'))
lock_entries = dynamoregistry.xquery('SELECT * FROM `detox_locks`')

def get_lock_size(name,site):
    try:
        dataset_name, block_name = Block.from_full_name(name)
    except:
        dataset_name = name
        block_name = 'NOPE'

    try:
        dsetObj = inventory.datasets[dataset_name]
    except:
        try:
            lock_size = 0
            pattern = re.compile(fnmatch.translate(name))                
            for d in inventory.datasets:
                if pattern.match(d.name):
                    for dr in d.replicas:
                        if dr.site.storage_type != Site.TYPE_DISK:
                            continue
                        if site is not None and site != dr.site.name:
                            continue
                        for br in dr.block_replicas:
                            lock_size += br.size*1e-12
            return lock_size
        except:
            return 0

    lock_size = 0
    if block_name == 'NOPE':
        for dr in dsetObj.replicas:
            if dr.site.storage_type != Site.TYPE_DISK:
                continue
            if site is not None and site != dr.site.name:
                continue
            for br in dr.block_replicas:
                lock_size += br.size*1e-12
    else:
        for dr in dsetObj.replicas:
            if dr.site.storage_type != Site.TYPE_DISK:
                continue
            if site is not None and site != dr.site.name:
                continue
            for br in dr.block_replicas:
                if br.block.name == block_name:
                    lock_size += br.size*1e-12

    return lock_size

item_id = []
item_name = []
item_site = []
item_from = []
item_until = []
item_user = []
item_comment = []
item_size = []

for e in lock_entries:
    #print e[0], e[1], e[2], e[4], e[5], e[6], e[9]
    item_id.append(int(e[0]))
    item_name.append(e[1])
    item_site.append(e[2])
    day = e[4].day
    if int(day) < 10:
        day = "0"+str(day)
    month = e[4].month
    if int(month) < 10:
        month = "0"+str(month)
    item_from.append("%s-%s-%s"%(e[4].year,month,day))
    day = e[5].day
    if int(day) < 10:
        day = "0"+str(day)
    month = e[5].month
    if int(month) < 10:
        month = "0"+str(month)
    item_until.append("%s-%s-%s"%(e[5].year,month,day))
    item_user.append(e[6])
    item_comment.append("-".join(str(e[9]).split()))
    item_size.append("%.2f"%get_lock_size(e[1],e[2]))

#print item_site

df = pd.DataFrame({'id':item_id,'item':item_name,'site':item_site,
                  'created':item_from,'expires':item_until,
                   'user':item_user,'comment':item_comment,'locksize':item_size})

df = df.sort_values(by=['expires'],ascending=False)

df.fillna(value="None", inplace=True)

df.to_csv("/tmp/detox_locks.txt", sep='\t', index=False, columns=["expires","created","user","site", "id","comment", "item", "locksize"])
df.to_csv("/home/dynamo/public_html/detox_locks.csv", sep=',', index=False, columns=["expires","created","user","site", "id","comment", "item", "locksize"])

with open("/tmp/detox_locks.txt", 'r') as f: 
    in_lines = f.readlines()
with open("/home/dynamo/public_html/detox_locks.txt", 'w') as f:
    counter = 0
    two_spaces = '   '
    eight_spaces = '      '
    f.write("Last updated: %s" % str(time.ctime(int(time.time()))) + ' EDT -- Sizes in TB \n')
    for line in in_lines:
        if counter == 0:
            out_line = "expires      created      user           site                id             size        comment                       item"
            f.write(out_line + '\n')
            counter += 1
            continue
        split_line = line.split()
        first_part = split_line[:3]
        first_outline = two_spaces.join(first_part)
        second_part = split_line[3:5]
        dynamic_spaces = ''
        while len(dynamic_spaces) < 20 - len(second_part[0]):
            dynamic_spaces += ' '
        second_outline = second_part[0] + dynamic_spaces + second_part[1]
        third_part = split_line[5:]
        third_outline = eight_spaces.join(third_part)
        dynamic_spaces2 = ''
        while len(dynamic_spaces2) < 15 - len(first_part[-1]):
            dynamic_spaces2 += ' '
        dynamic_spaces3 = ''
        while len(dynamic_spaces3) < 15 - len(second_part[-1]):
            dynamic_spaces3 += ' '
        dynamic_spaces4 = ' '
        while len(dynamic_spaces4) < 30 - len(third_part[0]):
            dynamic_spaces4 += ' '
        out_line = first_outline + dynamic_spaces2 + second_outline + dynamic_spaces3 + third_part[2] + "\t" + third_part[0] + dynamic_spaces4 + third_part[1]
        #print out_line
        f.write(out_line + '\n')


