"""Configuration management with precedence: CLI > env > YAML."""

import os
import yaml
import argparse
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


class Config:
    """Configuration manager with precedence: CLI > env > YAML."""
    
    ENV_PREFIX = "OS_LOADER_"
    
    def __init__(self, config_file: Optional[str] = None, cli_args: Optional[argparse.Namespace] = None):
        """Initialize configuration.
        
        Configuration precedence (highest to lowest):
        1. CLI arguments (cli_args)
        2. Environment variables (OS_LOADER_*)
        3. YAML configuration file (config_file)
        
        Args:
            config_file: Path to YAML configuration file (lowest precedence)
            cli_args: Parsed CLI arguments (highest precedence)
        """
        self.config: Dict[str, Any] = {}
        
        # Load from YAML file (lowest precedence)
        if config_file and Path(config_file).exists():
            with open(config_file, 'r') as f:
                self.config = yaml.safe_load(f) or {}
                self.config = self._trim_config_values(self.config)
        
        # Override with environment variables
        self._load_from_env()
        
        # Override with CLI arguments (highest precedence)
        if cli_args:
            self._load_from_cli(cli_args)
    
    def _load_from_env(self):
        """Load configuration from environment variables."""
        env_mapping = {
            'OS_LOADER_MEMGRAPH_HOST': ('memgraph', 'host'),
            'OS_LOADER_MEMGRAPH_PORT': ('memgraph', 'port'),
            'OS_LOADER_MEMGRAPH_USERNAME': ('memgraph', 'username'),
            'OS_LOADER_MEMGRAPH_PASSWORD': ('memgraph', 'password'),
            'OS_LOADER_OPENSEARCH_HOST': ('opensearch', 'host'),
            'OS_LOADER_OPENSEARCH_USE_SSL': ('opensearch', 'use_ssl'),
            'OS_LOADER_OPENSEARCH_VERIFY_CERTS': ('opensearch', 'verify_certs'),
            'OS_LOADER_OPENSEARCH_USERNAME': ('opensearch', 'username'),
            'OS_LOADER_OPENSEARCH_PASSWORD': ('opensearch', 'password'),
            'OS_LOADER_INDEX_SPEC_FILE': ('index_spec_file',),
            'OS_LOADER_CLEAR_EXISTING_INDICES': ('clear_existing_indices',),
            'OS_LOADER_ALLOW_INDEX_CREATION': ('allow_index_creation',),
            'OS_LOADER_SELECTED_INDICES': ('selected_indices',),
            'OS_LOADER_ABOUT_FILE': ('about_file',),
            'OS_LOADER_MODEL_FILES': ('model_files',),
            'OS_LOADER_TEST_MODE': ('test_mode',),
        }
        
        for env_var, path in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                # Trim whitespace before parsing
                value = value.strip()
                parsed_value = self._parse_env_value(value)
                if len(path) == 1:
                    self._set_nested(self.config, path[0], parsed_value)
                else:
                    if path[0] not in self.config:
                        self.config[path[0]] = {}
                    self._set_nested(self.config[path[0]], path[1], parsed_value)
    
    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable value to appropriate type."""
        # Value should already be trimmed, but trim again for safety
        value = value.strip()
        
        # Try boolean
        if value.lower() in ('true', '1', 'yes'):
            return True
        if value.lower() in ('false', '0', 'no'):
            return False
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try list (comma-separated)
        if ',' in value:
            return [v.strip() for v in value.split(',')]
        
        return value
    
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
    
    def _load_from_cli(self, args: argparse.Namespace):
        """Load configuration from CLI arguments."""
        if args.memgraph_host:
            if 'memgraph' not in self.config:
                self.config['memgraph'] = {}
            self.config['memgraph']['host'] = args.memgraph_host.strip() if isinstance(args.memgraph_host, str) else args.memgraph_host
        
        if args.memgraph_port:
            if 'memgraph' not in self.config:
                self.config['memgraph'] = {}
            self.config['memgraph']['port'] = args.memgraph_port
        
        if args.memgraph_username:
            if 'memgraph' not in self.config:
                self.config['memgraph'] = {}
            self.config['memgraph']['username'] = args.memgraph_username.strip() if isinstance(args.memgraph_username, str) else args.memgraph_username
        
        if args.memgraph_password:
            if 'memgraph' not in self.config:
                self.config['memgraph'] = {}
            self.config['memgraph']['password'] = args.memgraph_password.strip() if isinstance(args.memgraph_password, str) else args.memgraph_password
        
        if args.opensearch_host:
            if 'opensearch' not in self.config:
                self.config['opensearch'] = {}
            self.config['opensearch']['host'] = args.opensearch_host.strip() if isinstance(args.opensearch_host, str) else args.opensearch_host
        
        if args.opensearch_use_ssl is not None:
            if 'opensearch' not in self.config:
                self.config['opensearch'] = {}
            self.config['opensearch']['use_ssl'] = args.opensearch_use_ssl
        
        if args.opensearch_verify_certs is not None:
            if 'opensearch' not in self.config:
                self.config['opensearch'] = {}
            self.config['opensearch']['verify_certs'] = args.opensearch_verify_certs
        
        if args.opensearch_username:
            if 'opensearch' not in self.config:
                self.config['opensearch'] = {}
            self.config['opensearch']['username'] = args.opensearch_username.strip() if isinstance(args.opensearch_username, str) else args.opensearch_username
        
        if args.opensearch_password:
            if 'opensearch' not in self.config:
                self.config['opensearch'] = {}
            self.config['opensearch']['password'] = args.opensearch_password.strip() if isinstance(args.opensearch_password, str) else args.opensearch_password
        
        if args.index_spec_file:
            self.config['index_spec_file'] = args.index_spec_file.strip() if isinstance(args.index_spec_file, str) else args.index_spec_file
        
        # Use getattr to handle argparse.SUPPRESS (attribute won't exist if flag not provided)
        if hasattr(args, 'clear_existing_indices'):
            self.config['clear_existing_indices'] = args.clear_existing_indices
        
        if hasattr(args, 'allow_index_creation'):
            self.config['allow_index_creation'] = args.allow_index_creation
        
        if args.selected_indices:
            # Handle comma-separated list from CLI and trim each value
            if isinstance(args.selected_indices, str):
                self.config['selected_indices'] = [v.strip() for v in args.selected_indices.split(',') if v.strip()]
            elif isinstance(args.selected_indices, list):
                self.config['selected_indices'] = [v.strip() if isinstance(v, str) else v for v in args.selected_indices]
        
        if args.about_file:
            self.config['about_file'] = args.about_file.strip() if isinstance(args.about_file, str) else args.about_file
        
        if args.model_files:
            # Handle comma-separated list from CLI and trim each value
            if isinstance(args.model_files, str):
                self.config['model_files'] = [v.strip() for v in args.model_files.split(',') if v.strip()]
            elif isinstance(args.model_files, list):
                self.config['model_files'] = [v.strip() if isinstance(v, str) else v for v in args.model_files]
        
        # Use hasattr to handle argparse.SUPPRESS (attribute won't exist if flag not provided)
        if hasattr(args, 'test_mode'):
            self.config['test_mode'] = args.test_mode
    
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
        """Get test_mode setting.
        
        When enabled, each query will only run a single page of results
        to validate that all queries are syntactically correct.
        
        Returns:
            True if test_mode is enabled, False otherwise (default).
        """
        return self.config.get('test_mode', False)


def load_index_spec(index_spec_file: str) -> Dict[str, Any]:
    """Load index specification from YAML file."""
    with open(index_spec_file, 'r') as f:
        return yaml.safe_load(f) or {}

