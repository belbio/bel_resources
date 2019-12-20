#!/usr/bin/env bash

# This script will update all of the listed resources (terminologies, orthologies, etc)
# It is designed to run on it's own server.  After the server starts, a crontab
# entry "@reboot sleep 300 && /home/ubuntu/bin/update_resources.sh &" will
# start the update process.
#
# If the server is started in the hours below (02:00 - 06:00 server time - UTC), it
# will automatically shut the server down after finishing the resource update process.
# Outside of those hours, you can start the server without it automatically shutting itself
# down.

HOUR=`date +%H`

echo Hour $HOUR

# If this is run between UTC 02:00 - 06:00 - shut the server down
if [[ $HOUR -ge 2 ]]  && [[ $HOUR -le 6 ]]; then
    SHUTDOWN=1
else
    SHUTDOWN=0
fi

# Activate Python VirtualEnv
source "/home/ubuntu/resources/.venv/bin/activate"


# Update Namespaces
/home/ubuntu/resources/tools/namespaces/tax.py
/home/ubuntu/resources/tools/namespaces/chebi.py
# /home/ubuntu/resources/tools/namespaces/chembl.py
/home/ubuntu/resources/tools/namespaces/do.py
/home/ubuntu/resources/tools/namespaces/eg.py
/home/ubuntu/resources/tools/namespaces/go.py
/home/ubuntu/resources/tools/namespaces/hgnc.py
/home/ubuntu/resources/tools/namespaces/mesh.py
/home/ubuntu/resources/tools/namespaces/mgi.py
/home/ubuntu/resources/tools/namespaces/rgd.py
/home/ubuntu/resources/tools/namespaces/sp.py
/home/ubuntu/resources/tools/namespaces/zfin.py


# Update Orthologs
/home/ubuntu/resources/tools/orthologs/eg.py


# Update Backbone Nanopubs



# Ping Healthchecks.io
curl -s https://hchk.io/a4804e7f-0aef-43fd-a43c-a90003c7afce


# Shutdown server

if [ $SHUTDOWN = 1 ]; then
    echo Shutting Down
# aws ec2 stop-instances --dry-run --instance-ids i-05e567c33480ca869
    aws ec2 stop-instances --instance-ids i-05e567c33480ca869
fi
