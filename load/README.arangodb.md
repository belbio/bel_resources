# Notes on ArangoDB


## Queries

### Get namespace equivalence

    FOR vertex, edge
    IN 1..10
    ANY "equivalence_nodes/HGNC:A1BG" equivalence_edges
    FILTER vertex.namespace == "SP"
    RETURN vertex._key


## Misc

Shebang for scripts `#!/usr/bin/env arangosh --server.password "" --javascript.execute`

Example script:

    #!/usr/bin/env arangosh --server.password "" --javascript.execute

    db._useDatabase('bel');
    on = db._collection('ortholog_nodes');
    oe = db._collection('equivalence_nodes');
    on.ensureIndex({type: "hash", fields: ["tax_id"], sparse: true});
    oe.ensureIndex({type: "hash", fields: ["namespace"], sparse: true});
