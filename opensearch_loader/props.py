"""Properties configuration loader."""

import logging

logger = logging.getLogger("OpenSearchLoader")

# Default type mapping
DEFAULT_TYPE_MAPPING = {
    'string': 'String',
    'number': 'Float',
    'integer': 'Int',
    'boolean': 'Boolean',
    'array': 'Array',
    'list': 'Array',
    'object': 'Object',
    'datetime': 'DateTime',
    'date': 'Date',
    'TBD': 'String'
}


class Props:
    """Properties configuration loader - simplified to only handle type mapping."""
    
    def __init__(self):
        """Initialize Props with default type mapping."""
        # Use built-in default type mapping
        self.type_mapping = DEFAULT_TYPE_MAPPING.copy()
        self.id_fields = {}  # Will be populated from model file during schema initialization

