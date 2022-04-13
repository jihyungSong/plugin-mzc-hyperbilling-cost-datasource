import logging

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.model.data_source_model import PluginMetadata
from spaceone.cost_analysis.connector.mzc_hyperbilling_connector import MZCHyperBillingConnector

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(BaseManager):

    @staticmethod
    def init_response(options):
        plugin_metadata = PluginMetadata()
        plugin_metadata.validate()

        return {
            'metadata': plugin_metadata.to_primitive()
        }

    def verify_plugin(self, options, secret_data, schema):
        mzc_hb_connector: MZCHyperBillingConnector = self.locator.get_connector('MZCHyperBillingConnector')
        mzc_hb_connector.create_session(options, secret_data, schema)
