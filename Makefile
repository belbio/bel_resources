# Run make help to find out what the commands are

CREATE_TERMS = chebi.py
CREATE_TERMS2 = chebi.py do.py entrez-gene.py go.py hgnc.py mesh.py mgi.py rgd.py swissprot.py taxonomy.py

# Create databases (elasticsearch and arangodb)
.PHONY: create-databases
databases:
	./load/setup_db.py

.PHONY: collect-terms
collect-terms:
	cd terms; \
	for i in $(CREATE_TERMS); do \
		./$$i; \
	done

# Run all tests
.PHONY: test
test:
	./bin/runtests.sh


.PHONY: list  # ensures list is mis-identified with a file of the same name
list:
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'


.PHONY: help
help:
	@echo "List of commands"
	@echo "   deploy-{major|minor|patch} -- Deploy belmgr-plugin to npm and "
	@echo "      webeditor docker image to dockerhub"
	@echo "   help -- This listing "
	@echo "   list -- Automated listing of all targets"

.PHONY: check
check:
	@echo $(HOME)
	@echo $(gulp)
	@echo $(docker)
	@echo `cat VERSION`
	cd webeditor && echo `pwd`

