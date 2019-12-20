#!/usr/bin/env bash
#
# Setup new bel_resources environment
#
# run via 'bash <(curl -s https://raw.githubusercontent.com/belbio/bel_resources/master/tools/bin/initial-setup.sh)''
#

# Check for initial dependencies
hash poetry 2>/dev/null || { echo >&2 "I require Python3.6 ...  Aborting."; exit 1; }

# Clone repository if needed
ssh_status=$(ssh -o BatchMode=yes -o ConnectTimeout=5 git@github.com 2>&1)

if [[ $ssh_status == *"successfully authenticated"* ]] ; then
  clone_cmd="git clone git@github.com:belbio/bel_resources.git";
else
  clone_cmd="git clone https://github.com/belbio/bel_resources.git";
fi

if [ ! -d "bel_resources" ]; then
    $clone_cmd
else
    cd bel_resources;
    git pull;
    cd ..;
fi

# Set $HOME
cd bel_resources
HOME=$(pwd)
echo $HOME

# Setup virtualenv for Python and install required packages
poetry install
echo $HOME > .venv/lib/python3.7/site-packages/bel_resources.pth

echo "Do the following to finish setting up"
echo "  1. Change directory to bel_resources"
echo "  2. Run 'poetry shell' to enable the virtualenv"
echo "  3. Copy env.sample to .env and change any configuration in the belbio_conf file as needed"

echo
echo "   now you can run any of the commands under bel_resources/app or use 'make update' to run everything"
