# User under which dynamo runs
USER=dynamo

# Install target directory
INSTALLPATH=/usr/local/dynamo

# Configuration directory
CONFIGPATH=/etc/dynamo

# Archival directory
ARCHIVEPATH=/mnt/hadoop/dynamo/dynamo

# Temporary working directory
SPOOLPATH=/var/spool/dynamo

# Server log directory
LOGPATH=/var/log/dynamo

# Scheduler work directory
SCHEDULERPATH=/var/spool/dynamo/scheduler

# 1 -> Install daemons
DAEMONS=0

# Sequence file for scheduler daemon
SCHEDULERSEQ=cms.seq

# Httpd content directory
WEBPATH=/var/www

# Server database parameters
#SERVER_DB_WRITE_CNF=/etc/my.cnf.d/dynamo-write.cnf
SERVER_DB_WRITE_CNF=/etc/my.cnf
#SERVER_DB_WRITE_CNFGROUP=mysql
SERVER_DB_WRITE_CNFGROUP=mysql-dynamo
#SERVER_DB_WRITE_USER=
#SERVER_DB_WRITE_PASSWD=

#SERVER_DB_READ_CNF=/etc/my.cnf.d/dynamo.cnf
SERVER_DB_READ_CNF=/etc/my.cnf
#SERVER_DB_READ_CNFGROUP=mysql
SERVER_DB_READ_CNFGROUP=mysql-dynamo

SERVER_DB_HOST=localhost
SERVER_DB=dynamo
REGISTRY_DB=dynamoregister

# Registry host
#REGISTRY_HOST=t3serv017.mit.edu
REGISTRY_HOST=t3desk007.mit.edu
