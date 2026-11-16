import os
import glob
import yaml
from .config import Config
import logging
from prefect import flow
from .prefect_cli import repo_download
from bento.common.secret_manager import get_secret
from bento.common.utils import get_logger
from .cli import setup_logging, print_config
from .loader import Loader

MEMGRAPH_USER = "memgraph_user"
MEMGRAPH_ENDPOINT = "memgraph_endpoint"
MEMGRAPH_PASSWORD = "memgraph_password"
MODEL_REPO_URL = "model_repo_url"
MODEL_DESC = "model-desc"
MONOREPO_URL = "monorepo_url"
ENVIRONMENTS = "environments"
ES_HOST = "es_host"
MEMGRAPH_PORT = 7687
log = get_logger('OpenSearchLoader')

@flow(name="CRDC Data Hub OpenSearch Loader", log_prints=True)
def opensearch_loader_prefect(
    secret_name,
    model_repo_url,
    model_branch,
    mono_repo_url,
    monorepo_branch,
    about_file,
    indices_file,
    selected_indices
):
    secret = get_secret(secret_name)
    setup_logging(verbose=False)
    logger = logging.getLogger('OpenSearchLoader')

    model_repo = repo_download(model_repo_url, model_branch, logger)
    model_yaml_files = glob.glob(f'{model_repo}/{MODEL_DESC}/*model*.yaml')
    model_yml_files = glob.glob(f'{model_repo}/{MODEL_DESC}/*model*.yml')
    model_files = model_yaml_files + model_yml_files

    monorepo = repo_download(mono_repo_url, monorepo_branch, logger)
    about_file_path = os.path.join(monorepo, about_file)
    indices_file_path = os.path.join(monorepo, indices_file)

    config_object = {
        # Memgraph connection settings
        "memgraph": {
            "host": secret[MEMGRAPH_ENDPOINT],
            "port": MEMGRAPH_PORT,
            "username": secret[MEMGRAPH_USER],
            "password": secret[MEMGRAPH_PASSWORD],
        },
        # OpenSearch connection settings
        "opensearch": {
            "host": secret[ES_HOST],
            "use_ssl": True,
            "verify_certs": True,
        },
        # Index specification and processing settings
        "index_spec_file": indices_file_path,
        "clear_existing_indices": False,
        "allow_index_creation": True,
        "selected_indices": selected_indices,
        "about_file": about_file_path,
        "model_files": model_files,
    }

    config_file = "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_object, f, default_flow_style=False, sort_keys=False)

    config = Config(config_file=config_file)
    print_config(config)
    loader = Loader(config)
    try:
        loader.load()
        logger.info("Data loading completed")
    finally:
        loader.close()

    return 0


if __name__ == "__main__":
    opensearch_loader_prefect.serve(name="opensearch_loader")
