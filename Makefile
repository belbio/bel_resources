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


deploy_major:
	@echo Deploying major update
	bumpversion major
	@${deploy_commands}

deploy_minor:
	@echo Deploying minor update
	bumpversion minor
	@${deploy_commands}

deploy_patch:
	@echo Deploying patch update
	bumpversion --allow-dirty patch
	${deploy_commands}

# Autobuild the sphinx docs
livedocs:
	cd docs; sphinx-autobuild -q -p 0 --open-browser --delay 5 source build/html


clean_all:
	rm -f data/namespaces/*
	rm -f data/orthologs/*
	belc db elasticsearch --delete
	belc db arangodb --delete belns

load_all:
	tools/bin/update_namespaces.py
	tools/bin/update_orthologs.py
	tools/bin/load_elasticsearch.py
	tools/bin/load_arango.py


install:
	python3.6 -m venv .venv --prompt belres
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install --upgrade setuptools

	.venv/bin/pip install -r requirements.txt
	.venv/bin/pip install -r requirements-docs.txt


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

