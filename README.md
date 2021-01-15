# BEL Resource tools

This repository contains scripts to create standardized load files for BEL Resources for use with the BEL.bio API.

`Documentation <http://bel-resources.readthedocs.io/en/latest/>`_

## Install/setup

Run

    bin/



## Namespaces

The namespaces prefixes will preferentially use the identifiers derived from the identifiers-org/MIRIAM registry.

Additional biocontexts for these namespaces can be found here: https://github.com/prefixcommons/biocontext

## OWL terminologies

We can use OWL terminologies by converting them from OWL to [OBOGraph](https://douroucouli.wordpress.com/tag/json-owl-python-formats-bioinformatics/) and [OBOGraphs Github repo](https://github.com/geneontology/obographs).

### Convert OWL to OBO or OBOGraph

http://robot.obolibrary.org/

    robot convert --input fma.owl --output fma.obo --format obo --check false

    robot convert --input fma.owl --output fma.json --format json  # OBOGraph format

For the OBOGraph (https://github.com/geneontology/obographs/), you can process the nodes and edges as indicated in the overview of OBOGraph above.

If you use `robot` to convert to OBO format - you'll need to remove the 'owl-axioms' entry. You can use `grep -v ^owl-axioms <filename>` to do this, but the recommended approach is to use OBOGraph format instead.


### Alternatives for OWL processing

- https://github.com/RDFLib/pyLODE
- http://www.michelepasin.org/blog/2011/07/18/inspecting-an-ontology-with-rdflib/
- https://github.com/RDFLib/rdflib
