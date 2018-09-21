FROM sl:7
MAINTAINER Daniel Abercrombie <dabercro@mit.edu>

# Users
RUN useradd mysql && useradd dynamo

# Repositories
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm

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
    MySQL-python

# Install MariaDB
RUN mysql_install_db --user=mysql
RUN printf "mysqld_safe &\nsleep 5\nmysqladmin -u root password 'test'\nkill %%1\n" | bash

# Install certificate
RUN /etc/pki/tls/certs/make-dummy-cert /etc/pki/tls/certs/localhost.crt

# Will need to run 'mysqld_safe &' at startup for tests
