Namespaces
====================================

Overview
---------

This document describes how the Namespaces are processed.  Most Gene/RNA/Protein Namespaces also include orthology equivalences.

The namespaces are collected from their original databases, e.g. the ChEBI database. They are then converted into terminology and orthology JSON files as described in the `BELBio Schema Repository <https://github.com/belbio/schemas/tree/master/schemas>`.

The namespace individual entries are stored in Elasticsearch to provide strong, scalable, and fast search capabilities. The namespace equivalents and orthologies if available are stored in ArangoDB for graph queries.

Processing Overview
-----------------------

1. Run the Namespace script (for download and source file processing)
   to generate each Namespace <prefix>.jsonl.gz files in `tools/namespaces`

   1. First downloads original Namespace database files such as Entrez Gene and compresses them using gzip if not already gzipped.

      1. Only if it is newer than the prior download if determinable (some FTP servers don't make filedates available)
      2. If source file modification dates are indeterminable, it will download if the local file is older than 7 days (configurable in `belbio_conf.yml <http://bel.readthedocs.io/en/latest/configuration.html>`) - this is also adjustable per namespace script.

   2. Namespace script then processes the original source files to
      create the term data, equivalence and hierarchy
   3. Namespace script writes out each term into a gzipped
      `JSONL <http://jsonlines.org>`__ file using the terminology schema

2. Load namespaces into Elasticsearch

  1. After setting up Elasticsearch index
  2. Run `tools/load/load_elasticsearch.py -a`

3. Load equivalence files

  1. Generate equivalence files to load into ArangoDB from <term>.jsonl.gz files
  2. Load equivalence files into ArangoDB using `tools/load/load_arango.py`

5. Run Orthology scripts in tools/orthologs

  1. First Download Original Orthology database files and compress using gzip

    1. If it is newer than prior download if determinable (some FTP servers don't make file modification dates available) (orthology and terminology source files use same download location so will only download once if file(s) is used by both terminology and orthology scripts)
    2. If source file modification dates are indeterminable, it will download if the local file is older than 7 days (configurable in `belbio_conf.yml <http://bel.readthedocs.io/en/latest/configuration.html>`) - this is also adjustable per namespace script.

  2. Orthology script then processes the original source files to create the orthologous relationships
  3. Orthology script writes out each orthology into a gzipped `JSONL <http://jsonlines.org>`__ file using the orthologs schema
  4. Load orthology datasets into ArangoDB using `tools/load/load_arango.py`

Namespace Scripts
-------------------

Each namespace script is an independent script. Most do utilize some
utility functions from a utility library supporting the namespace and
orthology scripts.

.. note::

  Namespace values (NSArg) need to be quoted if they contain whitespace, comma or ')'. This is due to how BEL is parsed. An NSArg (namespace:term, e.g. namespace argument of a BEL function) is parsed by looking for an ALLCAPS namespace prefix, colon and then a term name. The parsing continues for the term name until we find a space, comma or end parenthesis ')'. If the term contains any of those characters, it has to be quoted using double-quotes.

.. note::

  Any character except an un-escaped double quote can be in the NSArg if it is quoted including spaces, commas and ')'.


Generally the namespace scripts do two main things:

1. Download the namespace source datafiles
2. Build the <term>.jsonl.gz file

All of the namespace scripts will be stored in the resource\_tools/namespace directory. Any \*.py files in that directory will be run to (re-)create the <namespace>.jsonl.gz files. The namespace scripts will create the <namespace>.jsonl.gz files in the resource\_tools/data/namespace directory. Any \*.jsonl.gz files will be loaded into Elasticsearch into the namespace index.

Taxonomy Terminology
--------------------

Taxonomy IDs are based on the `NCBI
Taxonomy <https://www.ncbi.nlm.nih.gov/taxonomy>`__. Taxonomy is treated
just like other terminologies with additional features of taxonomy\_name
object and taxonomy\_rank (kingdom, ..., genus, species). **The Taxonomy
terminology script has to be run first as it creates the
taxonomy\_labels.json.gz file which is used by all terminologies that
stores species\_id and species\_label in the <term>.jsonl.gz files**.

The taxonomy\_labels.json.gz file is a map (dictionary/hash) of all of
the TAX:<int> versus labels but only for taxonomy entries with
taxonomy\_rank: "species". **Note: It may be necessary to add labels to
this file for entries with non-species taxonomy\_rank as several
EntrezGene and SwissProt namespace entries do not have labels in this
file.**

The Taxonomy Namespace prefix is 'TAX'. Humans have the taxonomy id of
TAX:9606 with a custom label of 'human'.

Custom labels for specific species are sourced from the
*taxonomy\_labels.yaml* file adjacent to the taxonomy.py terminology
script. Custom labels file looks like:

::

    # Override taxonomy label
    # taxonomy_src_id: label
    ---
    9606: human
    10090: mouse
    10116: rat
    7955: zebrafish

Orthology Scripts
-----------------

Orthology Gene/Protein IDs collected from their source files need to be
converted to the canonical Namespace for Genes/Proteins (currently
Entrez Gene, prefix EG) prior to loading into ArangoDB **TODO**. This
will save time in processing through the equivalence edges.

Terminology and Orthology Schemas
---------------------------------

Schemas for terminologies and orthologies are kept in the `BELBio Schema
Repository <https://github.com/belbio/schemas/tree/master/schemas>`__.

Elasticsearch Index
-------------------

The Elasticsearch index map is in the es\_mapping\_term.yaml file and
the index is created using the setup\_es.py script. This setup\_es.py
script must be run before loading the terminologies the first time. It
will delete the *terms* index if it already exists. **Note: Need to
setup an A/B index option so that we can switch the index alias to a new
terms index.**

ArangoDB
--------

A 'bel' database is created and the following collections are added and
loaded:

1. ortholog\_nodes
2. ortholog\_edges
3. equivalence\_nodes
4. equivalence\_edges

These collections of nodes and edges allow equivalence and orthology
queries to be run against the bel ArangoDB database.
