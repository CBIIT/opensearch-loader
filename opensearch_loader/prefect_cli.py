from cli import print_config, setup_logging
import prefect.variables as Variable
import logging
from prefect import flow
from loader import Loader
from bento.common.secret_manager import get_secret


@flow(name="CRDC Data Hub OpenSearch Loader", log_prints=True)
def open_search_loader_prefect(

):
    setup_logging(verbose=False)
    logger = logging.getLogger(__name__)
    config = {}
    print_config(config)
    loader = Loader(config)
    try:
        loader.load()
        logger.info("Data loading completed successfully")
    finally:
        loader.close()
    
    return 0