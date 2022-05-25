import logging
import requests
import time
import copy

from spaceone.core.auth.jwt.jwt_util import JWTUtil
from spaceone.core.transaction import Transaction
from spaceone.core.connector import BaseConnector
from typing import List

from spaceone.cost_analysis.error import *

__all__ = ['MZCHyperBillingConnector']

_LOGGER = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    'Content-Type': 'application/json',
    'accept': 'application/json'
}

AUDIENCE = 'api.hb.cloudnoa.io'


class MZCHyperBillingConnector(BaseConnector):

    def __init__(self, transaction: Transaction, config: dict):
        super().__init__(transaction, config)
        self.endpoint = None
        self.account_id = None
        self.headers = copy.deepcopy(_DEFAULT_HEADERS)

    def create_session(self, options: dict, secret_data: dict, schema: str = None) -> None:
        self._check_secret_data(secret_data)

        client_email = secret_data['client_email']
        private_key = secret_data['private_key']
        jwt = self._generate_jwt(client_email, private_key)

        self.headers['Authorization'] = f'Bearer {jwt}'
        self.endpoint = secret_data['endpoint']
        self.account_id = secret_data['account_id']

    @staticmethod
    def _generate_jwt(client_email, private_key):
        now = int(time.time())
        payload = {
            'iat': now,
            'iss': client_email,
            'aud': AUDIENCE,
            'sub': client_email
        }

        return JWTUtil.encode(payload, private_key)

    @staticmethod
    def _check_secret_data(secret_data: dict) -> None:
        if 'client_email' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_email')

        if 'private_key' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.private_key')

        if 'endpoint' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.endpoint')

        if 'account_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.account_id')

    def get_cost_data(self, platform, month) -> dict:
        url = f'{self.endpoint}/summary'

        data = {
            'platform': platform,
            'account_id': self.account_id,
            'month': month,
            'kind': 'usage_detail'
        }

        _LOGGER.debug(f'[get_cost_data] ({self.account_id}) {url} => {data}')

        response = requests.get(url, params=data, headers=self.headers)

        if response.status_code == 200:
            response = response.json()

            if isinstance(response, dict) and 'errorCode' in response:
                _LOGGER.error(f'[get_cost_data] error message: {response}')
                raise ERROR_CONNECTOR_CALL_API(reason=str(response))
            else:
                return response
        else:
            _LOGGER.error(f'[get_cost_data] error code: {response.status_code}')
            try:
                error_message = response.json()
            except Exception as e:
                error_message = str(response)

            _LOGGER.error(f'[get_cost_data] error message: {error_message}')
            raise ERROR_CONNECTOR_CALL_API(reason=error_message)
