
This utility will download new versions of the taxonomy file
into the ../downloads directory.  It will read in taxonomy species
label overrides from the ./taxonomy_labels.yaml file and then
create a ../data/taxonomy.json file that will act as the load
file for taxonomy data into Elasticsearch and ArangoDB

Formatted JSON `cat ../data/taxonomy.json | jq . > t.json`
