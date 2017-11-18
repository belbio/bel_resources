#!/usr/bin/env arangosh --server.password "" --javascript.execute

db._useDatabase('bel');
on = db._collection('ortholog_nodes');
oe = db._collection('equivalence_nodes');
on.ensureIndex({type: "hash", fields: ["tax_id"], sparse: true});
oe.ensureIndex({type: "hash", fields: ["namespace"], sparse: true});

