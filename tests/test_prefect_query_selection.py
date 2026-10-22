"""Prefect parameter validation and shared loader configuration tests."""

import importlib
import unittest
from unittest.mock import MagicMock, patch

from opensearch_loader.config import Config as LoaderConfig


class PrefectQuerySelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # The flow discovers branch choices at import time. Keep tests offline.
        response = MagicMock()
        response.json.return_value = [{'name': 'main'}]
        with patch('requests.get', return_value=response), \
                patch('bento.common.utils.get_logger', return_value=MagicMock()):
            cls.module = importlib.import_module('opensearch_loader.prefect_cli')

    def build_config(self, selection):
        return self.module.Config(
            memgraph_host='localhost', memgraph_port=7687,
            memgraph_username=None, memgraph_password=None,
            opensearch_host='https://localhost:9200', indices_file='indices.yaml',
            about_file=None, model_files=[], selected_indices=selection,
        )

    def test_prefect_and_yaml_configuration_share_selection_semantics(self):
        selection = [{'participants_table': [' first ', 'second']}, ' samples_table ']
        flow_config = self.build_config(selection)
        config = LoaderConfig()
        config.config['selected_indices'] = selection
        self.assertEqual(flow_config.get_selected_indices(), config.get_selected_indices())
        for name in ['participants_table', 'samples_table']:
            self.assertEqual(flow_config.get_selected_update_queries(name),
                             config.get_selected_update_queries(name))
        self.assertIsNone(flow_config.get_selected_update_queries('samples_table'))
        self.assertEqual(flow_config.get_max_nesting_depth(), config.get_max_nesting_depth())

    def test_empty_selection_retains_all_indices_behavior(self):
        for selection in [None, []]:
            with self.subTest(selection=selection):
                config = self.build_config(selection)
                self.assertIsNone(config.get_selected_indices())
                self.assertIsNone(config.get_selected_update_queries('participants_table'))

    def flow_parameters(self):
        return dict(environment='dev_cdk', model_branch='main', backend_branch='main',
                    frontend_branch='main', about_file='about.yaml', indices_file='indices.yaml')

    def test_prefect_parameter_validation_preserves_plain_and_nested_entries(self):
        selection = [{'participants_table': ['first', 'second']}, 'samples_table']
        flow = self.module.opensearch_loader_prefect
        validated = flow.validate_parameters({**self.flow_parameters(),
                                              'selected_indices': selection})
        self.assertEqual(validated['selected_indices'], selection)
        schema = flow.parameters.model_dump()['properties']['selected_indices']
        self.assertEqual(schema['anyOf'][0]['type'], 'array')

    def test_flow_passes_selection_to_loader(self):
        selection = [{'participants_table': ['first']}, 'samples_table']
        secret = {self.module.ES_HOST: 'https://localhost:9200',
                  self.module.MEMGRAPH_ENDPOINT: 'localhost',
                  self.module.MEMGRAPH_USER: 'user', self.module.MEMGRAPH_PASSWORD: 'test'}
        with patch.object(self.module, 'repo_download', return_value='repository'), \
                patch.object(self.module.glob, 'glob', return_value=[]), \
                patch.object(self.module, 'Variables') as variables, \
                patch.object(self.module, 'get_secret', return_value=secret), \
                patch.object(self.module, 'setup_logging'), \
                patch.object(self.module, 'print_config'), \
                patch.object(self.module, 'Loader') as loader:
            variables.get.return_value = 'secret-name'
            result = self.module.opensearch_loader_prefect.fn(
                **self.flow_parameters(), selected_indices=selection)
        self.assertEqual(result, 0)
        config = loader.call_args.args[0]
        self.assertEqual(config.get_selected_indices(), ['participants_table', 'samples_table'])
        self.assertEqual(config.get_selected_update_queries('participants_table'), ['first'])
        self.assertIsNone(config.get_selected_update_queries('samples_table'))
        loader.return_value.load.assert_called_once()
        loader.return_value.close.assert_called_once()

    def test_empty_nested_query_list_is_rejected(self):
        with self.assertRaises(ValueError):
            self.build_config([{'participants_table': []}]).get_selected_indices()


if __name__ == '__main__':
    unittest.main()
