# Run make help to find out what the commands are

.PHONY: deploy_major deploy_minor deploy_patch list help tests
.PHONY: livedocs load_elasticsearch collect_terms clean_all load_all

# to get executable python files in the tool/terms directory
# find terms -perm +111 -name "*.py"

TERMS_SCRIPTS = $(shell find ./tools/terms -perm +111 -name "*.py")

define deploy_commands
    @echo "Update CHANGELOG"
    @echo "Create Github release and attach the gem file"

    git push
	git push --tags
endef


deploy_major: update_parsers
	@echo Deploying major update
	bumpversion major
	@${deploy_commands}

deploy_minor: update_parsers
	@echo Deploying minor update
	bumpversion minor
	@${deploy_commands}

deploy_patch: update_parsers
	@echo Deploying patch update
	bumpversion --allow-dirty patch
	${deploy_commands}

# Autobuild the sphinx docs
livedocs:
	cd docs; sphinx-autobuild -q -p 0 --open-browser --delay 5 source build/html


clean_all:
	rm data/namespaces/*
	rm data/orthologs/*
	bel db elasticsearch --clean
	bel db arangodb_belns --clean

load_all:
	tools/bin/update_namespaces.py
	tools/bin/update_orthologs.py
	tools/load/load_elasticsearch.py
	tools/load/load_arango.py

# Run all tests
tests:
	py.test -rs --cov=./tools --cov-report html --cov-config .coveragerc -c tests/pytest.ini --color=yes --durations=10 --flakes --pep8 tests


list:
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'


help:
	@echo ${CREATE_TERMS}
	@echo "List of commands"
	@echo "   deploy-{major|minor|patch} -- Deploy belmgr-plugin to npm and "
	@echo "      webeditor docker image to dockerhub"
	@echo "   help -- This listing "
	@echo "   list -- Automated listing of all targets"


check:
	@echo $(HOME)
	@echo $(gulp)
	@echo $(docker)
	@echo `cat VERSION`
	cd webeditor && echo `pwd`

