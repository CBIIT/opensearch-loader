"""Tests for multi-level nested field mapping helpers."""

import unittest

from opensearch_loader.nested_fields import (
    DEFAULT_MAX_NESTING_DEPTH,
    build_mapping_tree,
    collect_mapped_leaf_paths,
    is_path_mapped,
    validate_field_path,
)


class ValidateFieldPathTests(unittest.TestCase):
    def test_single_segment(self):
        self.assertEqual(validate_field_path('user_id', 5), ['user_id'])

    def test_multi_segment(self):
        self.assertEqual(
            validate_field_path('shipping.address.street', 5),
            ['shipping', 'address', 'street'],
        )

    def test_rejects_empty_segment(self):
        with self.assertRaises(ValueError):
            validate_field_path('a..b', 5)

    def test_rejects_excess_depth(self):
        path = '.'.join(f'level{i}' for i in range(6))
        with self.assertRaises(ValueError):
            validate_field_path(path, 5)


class BuildMappingTreeTests(unittest.TestCase):
    def test_single_level(self):
        mapping = build_mapping_tree({'keyword': ['user_id']}, 5)
        self.assertEqual(mapping, {'user_id': {'type': 'keyword'}})

    def test_two_level(self):
        mapping = build_mapping_tree({'keyword': ['metadata.category']}, 5)
        self.assertEqual(
            mapping,
            {
                'metadata': {
                    'type': 'nested',
                    'properties': {
                        'category': {'type': 'keyword'},
                    },
                },
            },
        )

    def test_three_level(self):
        mapping = build_mapping_tree(
            {'keyword': ['shipping.address.street']},
            5,
        )
        self.assertEqual(mapping['shipping']['type'], 'nested')
        self.assertEqual(mapping['shipping']['properties']['address']['type'], 'nested')
        self.assertEqual(
            mapping['shipping']['properties']['address']['properties']['street'],
            {'type': 'keyword'},
        )

    def test_parent_paths_are_nested_type(self):
        mapping = build_mapping_tree(
            {
                'keyword': [
                    'diagnoses.id',
                    'combined_filters.survival_filters.last_known_survival_status',
                ],
            },
            5,
        )
        self.assertEqual(mapping['diagnoses']['type'], 'nested')
        self.assertEqual(mapping['combined_filters']['type'], 'nested')
        self.assertEqual(
            mapping['combined_filters']['properties']['survival_filters']['type'],
            'nested',
        )

    def test_five_level(self):
        path = '.'.join(['a', 'b', 'c', 'd', 'e'])
        mapping = build_mapping_tree({'keyword': [path]}, 5)
        node = mapping
        for part in ['a', 'b', 'c', 'd']:
            node = node[part]['properties']
        self.assertEqual(node['e'], {'type': 'keyword'})

    def test_sibling_objects(self):
        mapping = build_mapping_tree(
            {
                'keyword': ['shipping.address.street', 'billing.city'],
            },
            5,
        )
        self.assertIn('shipping', mapping)
        self.assertIn('billing', mapping)

    def test_duplicate_path_raises(self):
        with self.assertRaises(ValueError):
            build_mapping_tree(
                {'keyword': ['metadata.category'], 'text': ['metadata.category']},
                5,
            )

    def test_scalar_and_nested_conflict(self):
        with self.assertRaises(ValueError):
            build_mapping_tree(
                {'keyword': ['shipping', 'shipping.address']},
                5,
            )

    def test_leaf_and_child_conflict(self):
        with self.assertRaises(ValueError):
            build_mapping_tree(
                {'keyword': ['shipping.address', 'shipping.address.street']},
                5,
            )


class IsPathMappedTests(unittest.TestCase):
    def setUp(self):
        self.mapping = build_mapping_tree(
            {
                'keyword': ['order_id', 'metadata.category', 'shipping.address.street'],
                'text': ['shipping.address.instructions'],
            },
            DEFAULT_MAX_NESTING_DEPTH,
        )

    def test_leaf_paths(self):
        self.assertTrue(is_path_mapped('order_id', self.mapping))
        self.assertTrue(is_path_mapped('metadata.category', self.mapping))
        self.assertTrue(is_path_mapped('shipping.address.street', self.mapping))

    def test_intermediate_object_paths(self):
        self.assertTrue(is_path_mapped('shipping', self.mapping))
        self.assertTrue(is_path_mapped('shipping.address', self.mapping))
        self.assertTrue(is_path_mapped('metadata', self.mapping))

    def test_unmapped_path(self):
        self.assertFalse(is_path_mapped('shipping.address.country', self.mapping))

    def test_collect_leaf_paths(self):
        leaves = collect_mapped_leaf_paths(self.mapping)
        self.assertIn('shipping.address.street', leaves)
        self.assertIn('metadata.category', leaves)
        self.assertNotIn('shipping', leaves)


class RegressionTests(unittest.TestCase):
    def test_existing_single_level_still_works(self):
        mapping = build_mapping_tree(
            {'keyword': ['product_id', 'metadata.category']},
            5,
        )
        self.assertEqual(
            mapping['metadata']['properties']['category'],
            {'type': 'keyword'},
        )
        self.assertTrue(is_path_mapped('metadata.category', mapping))
        self.assertTrue(is_path_mapped('metadata', mapping))
        self.assertTrue(is_path_mapped('product_id', mapping))


if __name__ == '__main__':
    unittest.main()
