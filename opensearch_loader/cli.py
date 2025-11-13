"""Command-line interface for the loader."""

import argparse
import logging
import sys
from pathlib import Path

from .config import Config
from .loader import Loader


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Set opensearch library loggers to WARNING to suppress INFO level HTTP request logs
    logging.getLogger('opensearch').setLevel(logging.WARNING)
    logging.getLogger('opensearchpy').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Load data from Memgraph to OpenSearch',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Configuration file
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration YAML file'
    )
    
    # Memgraph settings
    parser.add_argument(
        '--memgraph-host',
        type=str,
        help='Memgraph host (overrides config and env)'
    )
    parser.add_argument(
        '--memgraph-port',
        type=int,
        help='Memgraph port (overrides config and env)'
    )
    parser.add_argument(
        '--memgraph-username',
        type=str,
        help='Memgraph username (overrides config and env)'
    )
    parser.add_argument(
        '--memgraph-password',
        type=str,
        help='Memgraph password (overrides config and env)'
    )
    
    # OpenSearch settings
    parser.add_argument(
        '--opensearch-host',
        type=str,
        help='OpenSearch host (overrides config and env)'
    )
    parser.add_argument(
        '--opensearch-use-ssl',
        action='store_true',
        help='Use SSL for OpenSearch (overrides config and env)'
    )
    parser.add_argument(
        '--opensearch-no-ssl',
        dest='opensearch_use_ssl',
        action='store_false',
        help='Disable SSL for OpenSearch (overrides config and env)'
    )
    parser.add_argument(
        '--opensearch-verify-certs',
        action='store_true',
        help='Verify SSL certificates (overrides config and env)'
    )
    parser.add_argument(
        '--opensearch-no-verify-certs',
        dest='opensearch_verify_certs',
        action='store_false',
        help='Do not verify SSL certificates (overrides config and env)'
    )
    parser.add_argument(
        '--opensearch-username',
        type=str,
        help='OpenSearch username (overrides config and env)'
    )
    parser.add_argument(
        '--opensearch-password',
        type=str,
        help='OpenSearch password (overrides config and env)'
    )
    
    # Index settings
    parser.add_argument(
        '--index-spec-file',
        type=str,
        help='Path to index specification YAML file (overrides config and env)'
    )
    parser.add_argument(
        '--clear-existing-indices',
        action='store_true',
        help='Clear existing indices before starting (overrides config and env)'
    )
    parser.add_argument(
        '--no-clear-existing-indices',
        dest='clear_existing_indices',
        action='store_false',
        help='Do not clear existing indices before starting (overrides config and env)'
    )
    parser.add_argument(
        '--allow-index-creation',
        action='store_true',
        help='Allow creation of indices if they do not exist (overrides config and env)'
    )
    parser.add_argument(
        '--no-allow-index-creation',
        dest='allow_index_creation',
        action='store_false',
        help='Do not allow creation of indices if they do not exist (overrides config and env)'
    )
    parser.add_argument(
        '--selected-indices',
        type=str,
        help='Comma-separated list of index names to load (overrides config and env). Only indices in this list will be processed.'
    )
    
    # Other options
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(verbose=args.verbose)
    
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config_file = args.config
        if not config_file:
            # Try default config file
            default_config = Path('config.yaml')
            if default_config.exists():
                config_file = str(default_config)
        
        config = Config(config_file=config_file, cli_args=args)
        
        # Create and run loader
        loader = Loader(config)
        try:
            loader.load()
            logger.info("Data loading completed successfully")
        finally:
            loader.close()
        
        return 0
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=args.verbose)
        return 1


if __name__ == '__main__':
    sys.exit(main())

