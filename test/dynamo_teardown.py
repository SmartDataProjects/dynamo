#! /usr/bin/env python

import os
import subprocess

from dynamo import dataformat
from dynamo.core.executable import inventory


def shell():
    subprocess.check_call(
        """
        # This first line is only here so that it doesn't need reauthorized
        # every time there's in edit
        dynamo-exec-auth -u dynamo -x {script} --title teardown > /dev/null 2>&1
        su -c 'dynamo -t teardown -W {script}' dynamo > /dev/null 2>&1
        """.format(
            script=__file__.replace('.pyc', '.py')),
        shell=True)


def main(inv):
    for attr in ['partitions', 'sites', 'groups', 'datasets']:
        for key, obj in getattr(inv, attr).items():
            if key is not None:
                inv.delete(obj)


if __name__ == '__main__':
    if os.getuid():
        main(inventory)
    # We get here if we're running the script as root user (happens in docker all the time)
    else:
        shell()
