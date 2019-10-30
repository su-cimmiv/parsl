import logging
import time
from typing import Dict, Any, Sequence

from parsl.dataflow.executor_status import ExecutorStatus
from parsl.dataflow.job_error_handler import JobErrorHandler
from parsl.dataflow.strategy import Strategy
from parsl.executors.base import ParslExecutor
from parsl.providers.provider_base import JobStatus

logger = logging.getLogger(__name__)


class PollItem(ExecutorStatus):
    def __init__(self, executor: ParslExecutor):
        self._executor = executor
        if not executor.provider:
            raise ValueError("Executor with no provider passed to PollItem.__init__()")
        self._interval = executor.provider.status_polling_interval
        self._last_poll_time = 0
        self._status = {}

    def _should_poll(self, now: float):
        return now >= self._last_poll_time + self._interval

    def poll(self, now: float):
        if self._should_poll(now):
            logging.debug("Polling {}".format(self._executor))
            self._status = self._executor.status()
            self._last_poll_time = now

    @property
    def status(self) -> Dict[Any, JobStatus]:
        return self._status

    @property
    def executor(self) -> ParslExecutor:
        return self._executor

    def __repr__(self):
        return self._status.__repr__()


class TaskStatusPoller(object):
    def __init__(self, dfk: "parsl.dataflow.dflow.DataFlowKernel"):
        self._poll_items = []
        self._strategy = Strategy(dfk)
        self._error_handler = JobErrorHandler()

    def poll(self, tasks=None, kind=None):
        logging.debug("Polling")
        self._update_state()
        self._error_handler.run(self._poll_items)
        self._strategy.strategize(self._poll_items, tasks)

    def _update_state(self):
        now = time.time()
        for item in self._poll_items:
            item.poll(now)

    def add_executors(self, executors: Sequence[ParslExecutor]):
        for executor in executors:
            if executor.error_management_enabled and executor.provider \
                    and executor.status_polling_interval > 0:
                logging.debug("Adding executor {}".format(executor))
                self._poll_items.append(PollItem(executor))
        self._strategy.add_executors(executors)
