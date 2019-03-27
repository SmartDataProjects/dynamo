#!/usr/bin/env python
import time
import datetime
import random
import matplotlib
matplotlib.use('Agg')
import matplotlib.colors as mplcol
import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np
import pandas as pd

from dynamo.dataformat import Configuration
from dynamo.utils.interface.mysql import MySQL

from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

# # # # # # # #
# Query part  #
# # # # # # # #

nowtime = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y-%m-%d %H:%M:%S')

# Dynamo database: inbatch
dynamo = MySQL(Configuration(db = 'dynamo', user = 'dynamo', passwd = 'putpasswordhere'))
sites = dynamo.xquery('SELECT `id`, `name` FROM `sites`')
sitesdict = {}
for ide, name in sites:
    sitesdict[ide] = name
transfers = dynamo.xquery('SELECT tt.`source_id`, fs.`site_id`, f.`size` FROM `transfer_tasks` AS tt INNER JOIN file_subscriptions AS fs ON fs.`id` = tt.`subscription_id` INNER JOIN files as f on f.`id` = fs.`file_id` WHERE fs.`status`="inbatch" and fs.`delete`=0')


# Dynamo history database: exitcode
dynamohistory = MySQL(Configuration(db = 'dynamohistory', user = 'dynamo', passwd = 'putpasswordhere'))
historysites = dynamohistory.xquery('SELECT `id`, `name` FROM `sites`')
historysitesdict = {}
for ide, name in historysites:
    historysitesdict[ide] = name
historyxfers = dynamohistory.xquery('SELECT `source_id`, `destination_id`, `exitcode` FROM `file_transfers` WHERE `source_id` != 0 AND `completed` > NOW() - INTERVAL 7 DAY')


# # # # # # # # # # # # # # # # # # # # # # # #
# Calculating volume and error per connection #
# # # # # # # # # # # # # # # # # # # # # # # #

volume_per_connection = {}
total_volume_in_batch = 0

for source, dest, size in transfers:
    newkey = sitesdict[source] + '-' + sitesdict[dest]
    if newkey not in volume_per_connection:
        volume_per_connection[newkey] = 0
    try:
        volume_per_connection[newkey] += size/1.e12
        total_volume_in_batch += size/1.e12
    except:
        pass

errors_per_connection = {}
all_per_connection = {}
for source, dest, exitcode in historyxfers:
    newkey = historysitesdict[source] + '-' + historysitesdict[dest]
    if newkey not in errors_per_connection:
        errors_per_connection[newkey] = 0
        all_per_connection[newkey] = 0
    try:
        if exitcode != 0:
            errors_per_connection[newkey] += 1
        
        all_per_connection[newkey] += 1
    except:
        pass


for key, value in all_per_connection.iteritems():
    errors_per_connection[key] = errors_per_connection[key]/float(value)


# # # # # # # # #
# Preparing df  #
# # # # # # # # #

columns = ['source','destination','volume','color']
sources = []
destinations = []
volumes = []
colors = []
errorrates = []
sites = []

for key, value in sorted(volume_per_connection.iteritems()):
    sources.append(key.split('-')[0])
    destinations.append(key.split('-')[1])
    if sources[-1] not in sites:
        sites.append(sources[-1])
    if destinations[-1] not in sites:
        sites.append(destinations[-1])
    volumes.append(value)
    try:
        tmp = cm.RdYlGn(255-int(errors_per_connection[key]*100*256/100))
    except:
        # If a connection appears in batch for the first time, there is no 
        # history data for this connection yet, so the dictionary call will fail.
        tmp = cm.RdYlGn(255)
    rgb = tmp[:3] 
    colors.append('rgba(%.3f,%.3f,%.3f,0.5)'%(rgb[0],rgb[1],rgb[2]))        
    try:
        errorrates.append(errors_per_connection[key]*100.)
    except:
        errorrates.append(0.)

data = {'source': sources,  'volume': volumes, 'destination': destinations, 'color': colors, 'errors': errorrates}

# Massaging data into dataframe
df = pd.DataFrame(data)
df = df[df['volume']>0]
df = df.sort_values(by=['source'])

# Preparing labels column
labels = list(df.source.unique())
for dest in df.destination.unique():
    labels.append(dest)

while len(labels) < len(df.color):
    labels.append(np.nan)

while len(df.color) < len(labels):
    series = pd.Series([np.nan,np.nan,np.nan,np.nan,np.nan], index=['source', 'volume', 'destination', 'color', 'errors'])
    df = df.append(series,ignore_index=True)


# Preparing destinations
final_dests = []
counter = 0

for dest in df.destination.values.tolist():
    destfound = False
    for i in reversed(range(len(labels))):
        if dest == labels[i]:
            destfound = True
            final_dests.append(i)
            counter += 1
            break

# Massaging dataframe
df['label'] = labels
df['source'] = df.source.astype('category').cat.rename_categories(range(0, df.source.nunique()))
df = df.reset_index(drop=True)

tmpcol = df['color'].values.tolist()
for i in range(len(tmpcol)):
    try:
        if 'rgb' in tmpcol[i]:
            continue
    except:
        pass
    if np.isnan(tmpcol[i]):
        tmpcol[i] = 'rgba(0.000,0.408,0.216,0.5)'

df['color'] = tmpcol

while len(final_dests) < len(labels):
    final_dests.append(np.nan)

df['destination'] = final_dests


# Adjust linklabels

linklabels = df['volume'].values.tolist()
errrates = df['errors'].values.tolist()
for i in range(len(linklabels)):
    linklabels[i] = "error rate: %.2f%%" % errrates[i]
df['linklabels'] = linklabels


# # # # # # #
# Plotting  #
# # # # # # #

data_trace = dict(
    type='sankey',
    domain = dict(
      x =  [0,1],
      y =  [0,1]
    ),
    orientation = "h",
    valueformat = ".2f",
    valuesuffix = " TB",
    node = dict(
      pad = 10,
      thickness = 30,
      line = dict(
        color = "black",
        width = 0.5
      ),
      label =  df['label'],
      color = "#262C46"
    ),
    link = dict(
      source = df['source'],
      target = df['destination'],
      value = df['volume'],
      color = df['color'],
      label = df['linklabels']
  )
)

layout =  dict(
    title = "<b>Dynamo transfers in batch</b> <br>Total transfer volume: %.2f TB     Last updated: %s <br> Error rates correspond to last 3 days" % (total_volume_in_batch, nowtime),
    height = 942,
    width = 1200,
    font = dict(
      size = 11
    ),
)

fig = dict(data=[data_trace], layout=layout)

plot(fig, auto_open=False, filename='/var/spool/dynamo/dealermon/custom_plots/dynamo-sankey.html')
