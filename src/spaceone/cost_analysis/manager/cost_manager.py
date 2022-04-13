import copy
import logging
from datetime import datetime

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.connector.mzc_hyperbilling_connector import MZCHyperBillingConnector
from spaceone.cost_analysis.model.cost_model import Cost

_LOGGER = logging.getLogger(__name__)

PROVIDER_MAP = {
    'gcp': 'google_cloud'
}


class CostManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mzc_hb_connector: MZCHyperBillingConnector = self.locator.get_connector('MZCHyperBillingConnector')

    def get_data(self, options, secret_data, schema, task_options):
        self.mzc_hb_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        platform = task_options['platform']
        month = task_options['month']

        response = self.mzc_hb_connector.get_cost_data(platform, month)
        results = self._parse_response(response)

        yield self._make_cost_data(results, platform)

    def _make_cost_data(self, results, platform):
        costs_data = []

        for result in results:
            try:
                data = {
                    'cost': result['cost'],
                    'currency': 'USD',
                    'provider': PROVIDER_MAP[platform],
                    'product': result.get('product'),
                    'region_code': result.get('region_code'),
                    'account': result['project_id'],
                    'billed_at': datetime.strptime(result['month'], '%Y%m')
                }
            except Exception as e:
                _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
                raise e

            costs_data.append(data)

            # Excluded because schema validation is too slow
            # cost_data = Cost(data)
            # cost_data.validate()
            #
            # costs_data.append(cost_data.to_primitive())

        return costs_data

    @staticmethod
    def _check_task_options(task_options):
        if 'month' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.month')

        if 'platform' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.platform')

    @staticmethod
    def _parse_response(response):
        results = []
        for result in response:
            cost_data = {
                'month': result['month'],
                'additional_info': {}
            }
            for row1 in result.get('data', []):
                cost_data['project_id'] = row1['project_id']
                for row2 in row1.get('data', []):
                    cost_data['product'] = row2.get('service_name')
                    cost_data['region_code'] = row2.get('region_id')

                    cost_data['cost'] = 0
                    for cost_info in row2.get('costs', []):
                        cost_data['cost'] += cost_info['final_cost_in_usd']

                    results.append(copy.deepcopy(cost_data))

        return results
