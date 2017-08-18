mkdir downloads
mkdir -p data/terms
mkdir -p data/orthologs

virtualenv --prompt="[bel_res] " --python=/usr/local/bin/python3.6 .venv
source .venv/bin/activate

pip install -r ./requirements.txt
