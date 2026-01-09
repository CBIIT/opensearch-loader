import os
import glob
import yaml
import logging
import requests
import subprocess
import prefect.variables as Variables
from typing import Literal, Optional, Dict, Any, List
from cli import print_config, setup_logging
from prefect import flow
from loader import Loader
from bento.common.secret_manager import get_secret
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME

MEMGRAPH_USER = "memgraph_user"
MEMGRAPH_ENDPOINT = "memgraph_endpoint"
MEMGRAPH_PASSWORD = "memgraph_password"
MODEL_REPO_URL = "model_repo_url"
MODEL_DESC = "model-desc"
DROP_DOWN_CONFIG = "prefect_drop_down_config_opensearch_loader.yaml"
MONOREPO_URL = "monorepo_url"
ENVIRONMENTS = "environments"
ES_HOST = "es_host"
MEMGRAPH_PORT = 7687
log = get_logger('OpenSearchLoader')

class Config:
    
    def __init__(self, memgraph_host, memgraph_port, memgraph_username, memgraph_password, opensearch_host, indices_file, about_file, model_files, selected_indices):
        self.config: Dict[str, Any] = {}
        self.config["memgraph"] = {}
        self.config["opensearch"] = {}
        self.config["memgraph"]["host"] = memgraph_host
        self.config["memgraph"]["port"] = memgraph_port
        self.config["memgraph"]["username"] = memgraph_username
        self.config["memgraph"]["password"] = memgraph_password
        self.config["opensearch"]["host"] = opensearch_host
        self.config["opensearch"]["use_ssl"] = False
        self.config["opensearch"]["verify_certs"] = False
        self.config["index_spec_file"] = indices_file
        self.config["clear_existing_indices"] = False
        self.config["allow_index_creation"] = True
        self.config["about_file"] = about_file
        self.config["model_files"] = model_files
        self.config["selected_indices"] = selected_indices

        self.config = self._trim_config_values(self.config)
    
    def _trim_config_values(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively trim whitespace from string values in configuration."""
        if isinstance(config, dict):
            return {k: self._trim_config_values(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._trim_config_values(item) for item in config]
        elif isinstance(config, str):
            return config.strip()
        else:
            return config
    
    def _set_nested(self, d: Dict, key: str, value: Any):
        """Set nested dictionary value."""
        # Trim string values
        if isinstance(value, str):
            value = value.strip()
        d[key] = value
    
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.get(key, default)
    
    def get_memgraph_config(self) -> Dict[str, Any]:
        """Get Memgraph configuration."""
        return self.config.get('memgraph', {})
    
    def get_opensearch_config(self) -> Dict[str, Any]:
        """Get OpenSearch configuration."""
        return self.config.get('opensearch', {})
    
    def get_index_spec_file(self) -> Optional[str]:
        """Get index specification file path."""
        return self.config.get('index_spec_file')
    
    def get_clear_existing_indices(self) -> bool:
        """Get clear_existing_indices setting."""
        return self.config.get('clear_existing_indices', False)
    
    def get_allow_index_creation(self) -> bool:
        """Get allow_index_creation setting."""
        return self.config.get('allow_index_creation', True)
    
    def get_selected_indices(self) -> Optional[List[str]]:
        """Get selected_indices setting.
        
        Returns a list of index names with whitespace trimmed, or None if not set or empty.
        If None is returned, all indices should be processed.
        """
        selected = self.config.get('selected_indices')
        if selected is None:
            return None
        
        # Ensure it's a list and trim each value
        if isinstance(selected, list):
            # If empty list is provided, return None to process all indices
            if not selected:
                return None
            # Trim each index name and filter out empty strings
            trimmed = [v.strip() if isinstance(v, str) else str(v).strip() for v in selected if v]
            # If all values were empty strings, return None to process all indices
            return trimmed if trimmed else None
        elif isinstance(selected, str):
            # Handle string case (shouldn't happen with proper config, but defensive)
            trimmed = selected.strip()
            return [trimmed] if trimmed else None
        
        return None
    
    def get_about_file(self) -> Optional[str]:
        """Get about_file path."""
        return self.config.get('about_file')
    
    def get_model_files(self) -> Optional[List[str]]:
        """Get model_files list."""
        model_files = self.config.get('model_files')
        if model_files is None:
            return None
        if isinstance(model_files, list):
            return [v.strip() if isinstance(v, str) else str(v).strip() for v in model_files if v]
        elif isinstance(model_files, str):
            return [model_files.strip()] if model_files.strip() else None
        return None
    
    def get_test_mode(self) -> bool:
        return self.config.get('test_mode', False)

def get_github_branches(repo_url):
    # Remove .git if present
    if repo_url.endswith('.git'):
        repo_url = repo_url[:-4]
    # Extract owner and repo name
    parts = repo_url.rstrip('/').split('/')
    owner, repo = parts[-2], parts[-1]
    branches = []
    page = 1
    while True:
        api_url = f'https://api.github.com/repos/{owner}/{repo}/branches?per_page=100&page={page}'
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            branches.extend([branch['name'] for branch in data])
            if len(data) < 100:
                break
            page += 1
        except Exception as e:
            log.error(f"Error fetching branches from GitHub: {e}")
            break
    return branches


def repo_download(repo, version, logger):
    subprocess.run(['git', 'clone', repo])
    repo_folder = os.path.splitext(os.path.basename(repo))[0]
    subprocess.run(['git', '-C', repo_folder, 'checkout', version])
    logger.info(f"Finished cloning the data model repository from {repo} to {repo_folder}")
    return repo_folder


config_file = DROP_DOWN_CONFIG
with open(config_file, 'r') as file:
    config_drop_list = yaml.safe_load(file)
model_repo_url = config_drop_list.get(MODEL_REPO_URL)
monorepo_url = config_drop_list.get(MONOREPO_URL)
model_branch_choices = Literal[tuple(get_github_branches(model_repo_url))]
monorepo_branch_choices = Literal[tuple(get_github_branches(monorepo_url))]
env = config_drop_list[ENVIRONMENTS].keys()
environment_choices = Literal[tuple(list(env))]
@flow(name="CRDC Data Hub OpenSearch Loader", log_prints=True)
def opensearch_loader_prefect(
    environment: environment_choices, # type: ignore
    model_branch: model_branch_choices, # type: ignore
    monorepo_branch_choices: monorepo_branch_choices, # type: ignore
    about_file,
    indices_file,
    selected_indices

):
    setup_logging(verbose=False)
    logger = logging.getLogger('OpenSearchLoader')
    model_repo = repo_download(model_repo_url, model_branch, logger)
    model_yaml_files = glob.glob(f'{model_repo}/{MODEL_DESC}/*model*.yaml')
    model_yml_files = glob.glob(f'{model_repo}/{MODEL_DESC}/*model*.yml')
    model_files = model_yaml_files + model_yml_files
    monorepo = repo_download(monorepo_url, monorepo_branch_choices, logger)
    about_file_path = os.path.join(monorepo, about_file)
    indices_file_path = os.path.join(monorepo, indices_file)
    memgraph_secret_name = Variables.get(config_drop_list[ENVIRONMENTS][environment])
    secret = get_secret(memgraph_secret_name)
    opensearch_host = secret[ES_HOST]
    memgraph_endpoint_host = secret[MEMGRAPH_ENDPOINT]
    memgraph_user = secret[MEMGRAPH_USER]
    memgraph_password = secret[MEMGRAPH_PASSWORD]
    memgraph_port = MEMGRAPH_PORT
    config = Config(
        memgraph_host=memgraph_endpoint_host,
        memgraph_port=memgraph_port,
        memgraph_username=memgraph_user,
        memgraph_password=memgraph_password,
        opensearch_host=opensearch_host,
        indices_file=indices_file_path,
        about_file=about_file_path,
        model_files=model_files,
        selected_indices=selected_indices
    )
    print_config(config)
    loader = Loader(config)
    try:
        loader.load()
        logger.info("Data loading completed successfully")
    finally:
        loader.close()
    
    return 0

if __name__ == "__main__":
    # create your first deployment
    opensearch_loader_prefect.serve(name="opensearch_loader")