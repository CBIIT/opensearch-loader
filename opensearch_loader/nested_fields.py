"""Helpers for multi-level nested field mapping in index specifications."""

from typing import Dict, List, Any, Set

DEFAULT_MAX_NESTING_DEPTH = 5
MAX_NESTING_DEPTH_LIMIT = 5

VALID_FIELD_TYPES = {
    'keyword', 'text', 'search_as_you_type', 'long', 'integer',
    'double', 'float', 'boolean', 'date', 'object', 'nested',
}

DEFAULT_CONTAINER_FIELD_TYPE = 'nested'


def validate_field_path(path: str, max_depth: int) -> List[str]:
    """Split and validate a dot-notation field path from index mapping YAML.

    Args:
        path: Field path (e.g. 'shipping.address.street')
        max_depth: Maximum allowed nesting depth (number of segments)

    Returns:
        List of path segments

    Raises:
        ValueError: If path is invalid or exceeds max_depth
    """
    if not isinstance(path, str) or not path.strip():
        raise ValueError(f"Field name must be a non-empty string, got: {path!r}")

    parts = path.strip().split('.')
    if any(not segment for segment in parts):
        raise ValueError(f"Field path must not contain empty segments, got: '{path}'")

    if len(parts) > max_depth:
        raise ValueError(
            f"Field path '{path}' exceeds maximum nesting depth of {max_depth} "
            f"({len(parts)} levels)"
        )

    return parts


def _insert_path(properties: Dict[str, Dict[str, Any]], parts: List[str], field_type: str) -> None:
    """Insert a field path into an OpenSearch properties dict."""
    head, *tail = parts
    path_str = '.'.join(parts)

    if not tail:
        if head in properties:
            existing = properties[head]
            existing_type = existing.get('type')
            if (existing_type == 'object' or existing_type == 'nested') and 'properties' in existing:
                raise ValueError(
                    f"Cannot map '{path_str}' as a leaf field: "
                    f"'{head}' is already a nested field with child properties"
                )
        properties[head] = {'type': field_type}
        return

    if head in properties:
        existing = properties[head]
        existing_type = existing.get('type')
        if (existing_type != 'object' and existing_type != 'nested') or 'properties' not in existing:
            raise ValueError(
                f"Cannot have nested properties under '{head}': "
                f"it is already mapped as type '{existing.get('type')}'"
            )
    else:
        properties[head] = {
            'type': DEFAULT_CONTAINER_FIELD_TYPE,
            'properties': {},
        }

    _insert_path(properties[head]['properties'], tail, field_type)


def build_mapping_tree(
    fields_by_type: Dict[str, List[str]],
    max_depth: int,
) -> Dict[str, Dict[str, Any]]:
    """Convert grouped YAML mapping format to OpenSearch mapping properties.

    Dot notation in field names defines nested paths (mapping only).

    Args:
        fields_by_type: Dict mapping OpenSearch types to lists of field paths
        max_depth: Maximum allowed nesting depth

    Returns:
        OpenSearch properties dict for index mappings

    Raises:
        ValueError: If mapping is empty or invalid
    """
    if not fields_by_type:
        raise ValueError("Mapping configuration cannot be empty")

    result: Dict[str, Dict[str, Any]] = {}
    all_field_paths: Set[str] = set()

    for field_type, fields in fields_by_type.items():
        if not isinstance(fields, list):
            raise ValueError(f"Fields for type '{field_type}' must be a list")

        if field_type not in VALID_FIELD_TYPES:
            raise ValueError(
                f"Invalid field type '{field_type}'. Valid types: {VALID_FIELD_TYPES}"
            )

        for field in fields:
            parts = validate_field_path(field, max_depth)

            if field.strip() in all_field_paths:
                raise ValueError(f"Duplicate field definition: '{field.strip()}'")
            all_field_paths.add(field.strip())

            _insert_path(result, parts, field_type)

    if not result:
        raise ValueError("Mapping must contain at least one field")

    return result


def is_path_mapped(field_path: str, mapping: Dict[str, Dict[str, Any]]) -> bool:
    """Check whether a document field path is covered by a parsed mapping tree.

    Args:
        field_path: Dot-notation path from document field extraction
        mapping: Parsed OpenSearch properties dict from build_mapping_tree

    Returns:
        True if the path is mapped (leaf or intermediate object node)
    """
    if not field_path:
        return False

    parts = field_path.split('.')
    current = mapping

    for i, part in enumerate(parts):
        if part not in current:
            return False

        cfg = current[part]
        is_last = i == len(parts) - 1
        cfg_type = cfg.get('type')

        if is_last:
            if (cfg_type == 'object' or cfg_type == 'nested') and 'properties' in cfg:
                return True
            return cfg_type is not None

        if (cfg_type != 'object' and cfg_type != 'nested') or 'properties' not in cfg:
            return False

        current = cfg['properties']

    return False


def collect_mapped_leaf_paths(mapping: Dict[str, Dict[str, Any]], prefix: str = '') -> Set[str]:
    """Collect dot-notation paths for all mapped leaf fields.

    Args:
        mapping: Parsed OpenSearch properties dict
        prefix: Path prefix for recursion

    Returns:
        Set of leaf field paths
    """
    paths: Set[str] = set()

    for name, cfg in mapping.items():
        path = f"{prefix}.{name}" if prefix else name
        cfg_type = cfg.get('type')

        if (cfg_type == 'object' or cfg_type == 'nested') and 'properties' in cfg:
            paths.update(collect_mapped_leaf_paths(cfg['properties'], path))
        else:
            paths.add(path)

    return paths
