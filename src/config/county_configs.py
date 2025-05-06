import logging
from importlib import import_module

logger = logging.getLogger(__name__)

def get_county_config(county: str) -> dict:
    """Load county-specific configuration."""
    try:
        module = import_module(f'src.config.{county}_config')
        config = module.CONFIG
        logger.info(f"Loaded configuration for county: {county}")
        return config
    except ImportError:
        logger.error(f"Configuration for county '{county}' not found")
        raise
    except AttributeError:
        logger.error(f"Invalid configuration format for county '{county}'")
        raise