FROM sl:7
MAINTAINER Daniel Abercrombie <dabercro@mit.edu>

# Users
RUN useradd mysql && useradd dynamo -u 500

# Repositories
RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

# Installation of packages
RUN yum -y install \
    initscripts \
    lighttpd \
    mariadb \
    mariadb-server \
    fetch-crl \
    python-flup \
    python-fts \
    rrdtool-python \
    condor-python \
    python-matplotlib \
    python-requests \
    MySQL-python \
    python-pip

# Install MariaDB
# Will need to run 'mysqld_safe &' at startup for tests
RUN printf "mysql_install_db --user=mysql\nmysqld_safe &\nsleep 5\nmysqladmin -u root password 'test'\nkill %%1\n" | bash

# Stuff below is not used by dynamo, but useful for tests
RUN pip install -U 'pip==18.0' 'cmstoolbox==0.11.0'
