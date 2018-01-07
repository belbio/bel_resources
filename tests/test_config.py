import re

import bel_nanopub.Config as Config


def test_get_current_module_dir():

    _dir = Config.get_current_module_dir()
    assert re.search('bel_nanopub', _dir)


def test_get_belbio_conf_files():

    (conf_fn, secrets_fn) = Config.get_belbio_conf_files()
    assert re.search('tests/belbio_conf.yaml', conf_fn)
    assert re.search('tests/belbio_secrets.yaml', secrets_fn)


def test_config():

    config = Config.load_configuration()
    assert config['api'] == 'https://api.bel.bio/v1'


def test_merge_config():

    config = Config.load_configuration()
    override_config = {'bel_api': {'servers': {'server_type': 'DEV2'}}}
    new_config = Config.merge_config(config, override_config=override_config)

    assert config['bel_api']['servers']['server_type'] == 'DEV'
    assert new_config['bel_api']['servers']['server_type'] == 'DEV2'
