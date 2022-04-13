import logging
from datetime import datetime, timedelta
from dateutil import rrule

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.connector.mzc_hyperbilling_connector import MZCHyperBillingConnector
from spaceone.cost_analysis.model.job_model import Tasks

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ['gcp']


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mzc_hb_collector: MZCHyperBillingConnector = self.locator.get_connector('MZCHyperBillingConnector')

    def get_tasks(self, options, secret_data, schema, start, last_synchronized_at):
        self.mzc_hb_collector.create_session(options, secret_data, schema)

        if start:
            start_time: datetime = start
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        months = self._get_months_from_start_time(start_time)

        _LOGGER.debug(f'[get_tasks] all months: {months}')
        tasks = []
        changed = []
        for month in months:
            for platform in PLATFORMS:
                tasks.append({
                    'task_options': {
                        'platform': platform,
                        'month': month
                    }
                })

        changed.append({
            'start': start_time
        })

        _LOGGER.debug(f'[get_tasks] tasks: {tasks}')
        _LOGGER.debug(f'[get_tasks] changed: {changed}')

        tasks = Tasks({'tasks': tasks, 'changed': changed})

        tasks.validate()
        return tasks.to_primitive()

    @staticmethod
    def _get_months_from_start_time(start_time):
        months = []
        now = datetime.utcnow()
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_time, until=now):
            months.append(dt.strftime('%Y%m'))

        return months
