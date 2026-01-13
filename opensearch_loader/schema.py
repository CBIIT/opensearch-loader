"""Schema parser for model YAML files."""

import os
import yaml
import logging
from typing import Dict, Any, List, Set, Optional
from .props import Props

logger = logging.getLogger(__name__)

# Constants
NODES = 'Nodes'
KEY = "Key"
RELATIONSHIPS = 'Relationships'
PROPERTIES = 'Props'
PROP_DEFINITIONS = 'PropDefinitions'
DEFAULT_TYPE = 'String'
PROP_TYPE = 'Type'
PROP_ENUM = 'Enum'
DESCRIPTION = 'Desc'
REQUIRED = 'Req'
PRIVATE = 'Private'
ENUM = 'enum'


class Schema:
    """Schema parser for model YAML files."""
    
    def __init__(self, yaml_files: List[str], props: Props):
        """Initialize Schema from model YAML files.
        
        Args:
            yaml_files: List of paths to model YAML files
            props: Props instance for configuration
        """
        if not isinstance(props, Props):
            raise AssertionError("props must be an instance of Props")
        
        self.props = props
        
        if not yaml_files:
            raise ValueError('File list is empty, could not initialize Schema object!')
        
        # Validate all files exist
        for data_file in yaml_files:
            if not os.path.isfile(data_file):
                raise ValueError(f'File "{data_file}" does not exist')
        
        # Load and merge all schema files
        self.org_schema = {}
        for a_file in yaml_files:
            try:
                logger.info(f'Reading schema file: {a_file} ...')
                if os.path.isfile(a_file):
                    with open(a_file) as schema_file:
                        schema = yaml.safe_load(schema_file)
                        if schema:
                            self.org_schema.update(schema)
            except Exception as e:
                logger.exception(e)
        
        # Process nodes and relationships
        self.nodes = {}
        self.relationships = {}
        self.relationship_props = {}
        self.num_relationship = 0
        
        logger.debug("-------------processing nodes-----------------")
        if NODES not in self.org_schema:
            logger.error('Can\'t load any nodes!')
            raise ValueError('No nodes found in schema')
        
        if PROP_DEFINITIONS not in self.org_schema:
            logger.error('Can\'t load any properties!')
            raise ValueError('No property definitions found in schema')
        
        # Process nodes
        for key, value in self.org_schema[NODES].items():
            # Assume all keys start with '_' are not regular nodes
            if not key.startswith('_'):
                self.process_node(key, value)
        
        logger.debug("-------------processing edges-----------------")
        # Process relationships
        if RELATIONSHIPS in self.org_schema:
            for key, value in self.org_schema[RELATIONSHIPS].items():
                # Assume all keys start with '_' are not regular nodes
                if not key.startswith('_'):
                    self.process_node(key, value, True)
                    self.num_relationship += self.process_edges(key, value)
        
        # Always derive ID fields from model file
        id_fields = {}
        for node_type in self.org_schema[NODES]:
            node_id_list = self.get_node_id(node_type)
            if len(node_id_list) == 1:
                id_fields[node_type] = node_id_list[0]
                logger.info(f"Derived ID field {node_id_list[0]} for node {node_type} from model file")
            elif len(node_id_list) > 1:
                raise ValueError(f"More than one key property found for node {node_type}: {node_id_list}")
            elif len(node_id_list) == 0:
                logger.debug(f"No ID field found for node {node_type} in model file")
        
        # Store derived ID fields in props
        if id_fields:
            for node_type, id_field in id_fields.items():
                self.props.id_fields[node_type] = id_field
    
    def get_node_id(self, node_type: str) -> List[str]:
        """Get ID field(s) for a node type.
        
        Args:
            node_type: Name of the node type
            
        Returns:
            List of ID field names
        """
        node_id_list = []
        if node_type in self.org_schema[NODES]:
            node_def = self.org_schema[NODES][node_type]
            if PROPERTIES in node_def and node_def[PROPERTIES] is not None:
                for prop in node_def[PROPERTIES]:
                    if prop in self.org_schema[PROP_DEFINITIONS]:
                        prop_def = self.org_schema[PROP_DEFINITIONS][prop]
                        if KEY in prop_def and prop_def[KEY]:
                            node_id_list.append(prop)
        return node_id_list
    
    def _process_properties(self, desc: Dict[str, Any]) -> Dict[str, Any]:
        """Gather properties from description.
        
        Args:
            desc: Description dictionary containing properties
            
        Returns:
            Dictionary with PROPERTIES, REQUIRED, and PRIVATE keys
        """
        props = {}
        required = set()
        private = set()
        
        if PROPERTIES in desc and desc[PROPERTIES] is not None:
            for prop in desc[PROPERTIES]:
                prop_type = self.get_type(prop)
                props[prop] = prop_type
                
                if self.is_required_prop(prop):
                    required.add(prop)
                if self.is_private_prop(prop):
                    private.add(prop)
        
        return {PROPERTIES: props, REQUIRED: required, PRIVATE: private}
    
    def process_node(self, name: str, desc: Dict[str, Any], is_relationship: bool = False):
        """Process input node/relationship properties and save it in self.nodes.
        
        Args:
            name: Node/relationship name
            desc: Description dictionary
            is_relationship: Whether this is a relationship
        """
        properties = self._process_properties(desc)
        
        # All nodes and relationships that have properties will be saved to self.nodes
        # Relationship without properties will be ignored
        if properties[PROPERTIES] or not is_relationship:
            self.nodes[name] = properties
    
    def process_edges(self, name: str, desc: Dict[str, Any]) -> int:
        """Process relationship edges.
        
        Args:
            name: Relationship name
            desc: Description dictionary
            
        Returns:
            Number of edges processed
        """
        # Simplified version - just process properties
        # Full relationship processing would be more complex
        properties = self._process_properties(desc)
        self.relationship_props[name] = properties
        return 0  # Simplified - return 0 for now
    
    def is_required_prop(self, name: str) -> bool:
        """Check if a property is required.
        
        Args:
            name: Property name
            
        Returns:
            True if required, False otherwise
        """
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            result = prop.get(REQUIRED, False)
            result = str(result).lower()
            if result == "true" or result == "yes":
                return True
        return False
    
    def is_private_prop(self, name: str) -> bool:
        """Check if a property is private.
        
        Args:
            name: Property name
            
        Returns:
            True if private, False otherwise
        """
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            return prop.get(PRIVATE, False)
        return False
    
    def get_type(self, name: str) -> Dict[str, Any]:
        """Get property type information.
        
        Args:
            name: Property name
            
        Returns:
            Dictionary with PROP_TYPE, DESCRIPTION, REQUIRED, and optionally ENUM
        """
        result = {PROP_TYPE: DEFAULT_TYPE}
        
        if name in self.org_schema[PROP_DEFINITIONS]:
            prop = self.org_schema[PROP_DEFINITIONS][name]
            result[DESCRIPTION] = prop.get(DESCRIPTION, '')
            result[REQUIRED] = prop.get(REQUIRED, False)
            
            # Get type
            if PROP_TYPE in prop:
                prop_type = prop[PROP_TYPE]
                if isinstance(prop_type, str):
                    result[PROP_TYPE] = self.map_type(prop_type)
                elif isinstance(prop_type, list):
                    # List means enum
                    enum = set()
                    for t in prop_type:
                        enum.add(t)
                    if len(enum) > 0:
                        result[ENUM] = enum
                        result[PROP_TYPE] = DEFAULT_TYPE
            elif PROP_ENUM in prop:
                # Enum property
                enum_val = prop[PROP_ENUM]
                if isinstance(enum_val, list):
                    enum = set()
                    for t in enum_val:
                        enum.add(t)
                    if len(enum) > 0:
                        result[ENUM] = enum
                        result[PROP_TYPE] = PROP_ENUM
        
        return result
    
    def map_type(self, type_name: str) -> str:
        """Map type name using type mapping from props.
        
        Args:
            type_name: Type name to map
            
        Returns:
            Mapped type name or DEFAULT_TYPE
        """
        mapping = self.props.type_mapping
        if type_name in mapping:
            return mapping[type_name]
        else:
            logger.debug(f'Type: "{type_name}" has no mapping, use default type: "{DEFAULT_TYPE}"')
            return DEFAULT_TYPE
    
    def plural(self, word: str) -> str:
        """Get plural form of a word.
        
        Args:
            word: Word to pluralize (node name)
            
        Returns:
            The word itself (node name) - no pluralization applied
        """
        return word

