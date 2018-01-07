Quickstart
=============

1. `git clone git@github.com:belbio/bel_resources.git` to clone the BEL Resources project locally
2. Make sure BEL.bio API is running with the elasticsearch and arangodb containers - `Instructions <http://apidocs.bel.bio/install.html>`_
3. `make install` will setup a virtualenv and pip install the requirements
4. Setup your configuration file (:doc:`configuration`)
5. `make load_all` will download remote namespace/orthology files, convert into load-able formats and then load into elasticsearch and arangodb (:doc:`namespaces`)
6. `make clean_all` will clean out all loaded namespaces and orthologies

Remote database files (e.g. Entrez Gene, SwissProt) are downloaded to <repo_dir>/downloads (gzipped). BEL Namespace files for loading into elasticsearch are created from source database files and stored in <repo_dir>/data/namespaces. Orthology files are stored in <repo_dir>/data/orthologs.

