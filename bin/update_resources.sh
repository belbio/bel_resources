#!/usr/bin/env bash

# Activate Python VirtualEnv
source "/home/ubuntu/bel_resources_bep/.venv/bin/activate"

# Run First!!! - creates tax labels file used by other app
/home/ubuntu/bel_resources_bep/app/namespaces/tax.py

# Add virtual namespaces
/home/ubuntu/bel_resources_bep/app/namespaces/virtuals.py

# Sync files to S3
/home/ubuntu/.local/bin/aws s3 sync --quiet /data/bel_resources/resources_v2 s3://resources.bel.bio/resources_v2

# Update Namespaces
/home/ubuntu/bel_resources_bep/app/namespaces/chebi.py
/home/ubuntu/bel_resources_bep/app/namespaces/do.py
/home/ubuntu/bel_resources_bep/app/namespaces/eg.py
/home/ubuntu/bel_resources_bep/app/namespaces/go.py
/home/ubuntu/bel_resources_bep/app/namespaces/hgnc.py
/home/ubuntu/bel_resources_bep/app/namespaces/mesh.py
/home/ubuntu/bel_resources_bep/app/namespaces/mgi.py
/home/ubuntu/bel_resources_bep/app/namespaces/rgd.py
/home/ubuntu/bel_resources_bep/app/namespaces/sp.py
/home/ubuntu/bel_resources_bep/app/namespaces/zfin.py

# Only run if new files -- TODO figure out how to automate this
# /home/ubuntu/bel_resources/app/namespaces/chembl.py

# Update Orthologs
/home/ubuntu/bel_resources_bep/app/orthologs/eg.py
/home/ubuntu/bel_resources_bep/app/orthologs/hgnc.py

# Update Backbone Nanopubs
/home/ubuntu/bel_resources_bep/app/backbone/gene2protein.py

# Sync files to S3
/home/ubuntu/.local/bin/aws s3 sync --quiet /data/bel_resources/resources_v2 s3://resources.bel.bio/resources_v2


# Ping Healthchecks.io
curl -s https://hchk.io/a4804e7f-0aef-43fd-a43c-a90003c7afce
