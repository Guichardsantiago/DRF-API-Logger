import logging
from queue import Queue, Full
import time
from django.conf import settings
from django.db import transaction
from threading import Thread
from django.db.utils import OperationalError

from drf_api_logger.models import APILogsModel

logger = logging.getLogger(__name__)


class InsertLogIntoDatabase(Thread):

    def __init__(self):
        super().__init__()

        self.DRF_API_LOGGER_DEFAULT_DATABASE = 'default'
        if hasattr(settings, 'DRF_API_LOGGER_DEFAULT_DATABASE'):
            self.DRF_API_LOGGER_DEFAULT_DATABASE = settings.DRF_API_LOGGER_DEFAULT_DATABASE

        self.DRF_LOGGER_QUEUE_MAX_SIZE = 1000  # Default queue size 1000
        if hasattr(settings, 'DRF_LOGGER_QUEUE_MAX_SIZE'):
            self.DRF_LOGGER_QUEUE_MAX_SIZE = settings.DRF_LOGGER_QUEUE_MAX_SIZE

        if self.DRF_LOGGER_QUEUE_MAX_SIZE < 1:
            raise Exception("""
            DRF API LOGGER EXCEPTION
            Value of DRF_LOGGER_QUEUE_MAX_SIZE must be greater than 0
            """)

        self.DRF_LOGGER_INTERVAL = 10  # Default DB insertion interval is 10 seconds.
        if hasattr(settings, 'DRF_LOGGER_INTERVAL'):
            self.DRF_LOGGER_INTERVAL = settings.DRF_LOGGER_INTERVAL

            if self.DRF_LOGGER_INTERVAL < 1:
                raise Exception("""
                DRF API LOGGER EXCEPTION
                Value of DRF_LOGGER_INTERVAL must be greater than 0
                """)

        self.DRF_API_LOGGER_DROP_ON_OVERFLOW = True
        if hasattr(settings, 'DRF_API_LOGGER_DROP_ON_OVERFLOW'):
            self.DRF_API_LOGGER_DROP_ON_OVERFLOW = settings.DRF_API_LOGGER_DROP_ON_OVERFLOW

        self.DRF_API_LOGGER_PUT_TIMEOUT = 0.1
        if hasattr(settings, 'DRF_API_LOGGER_PUT_TIMEOUT'):
            self.DRF_API_LOGGER_PUT_TIMEOUT = settings.DRF_API_LOGGER_PUT_TIMEOUT

        self.DRF_API_LOGGER_DROPPED_WARN_EVERY = 1000
        if hasattr(settings, 'DRF_API_LOGGER_DROPPED_WARN_EVERY'):
            self.DRF_API_LOGGER_DROPPED_WARN_EVERY = settings.DRF_API_LOGGER_DROPPED_WARN_EVERY

        self.DRF_LOGGER_BULK_BATCH_SIZE = 500
        if hasattr(settings, 'DRF_LOGGER_BULK_BATCH_SIZE'):
            self.DRF_LOGGER_BULK_BATCH_SIZE = settings.DRF_LOGGER_BULK_BATCH_SIZE

        if self.DRF_LOGGER_BULK_BATCH_SIZE < 1:
            raise Exception("""
            DRF API LOGGER EXCEPTION
            Value of DRF_LOGGER_BULK_BATCH_SIZE must be greater than 0
            """)

        self.DRF_LOGGER_MAX_RETRIES = 3
        if hasattr(settings, 'DRF_LOGGER_MAX_RETRIES'):
            self.DRF_LOGGER_MAX_RETRIES = settings.DRF_LOGGER_MAX_RETRIES

        if self.DRF_LOGGER_MAX_RETRIES < 1:
            raise Exception("""
            DRF API LOGGER EXCEPTION
            Value of DRF_LOGGER_MAX_RETRIES must be greater than 0
            """)

        self._queue = Queue(maxsize=self.DRF_LOGGER_QUEUE_MAX_SIZE)
        self._dropped = 0

    @property
    def dropped_count(self):
        return self._dropped

    def run(self) -> None:
        self.start_queue_process()

    def put_log_data(self, data):
        entry = APILogsModel(**data)
        if self.DRF_API_LOGGER_DROP_ON_OVERFLOW:
            try:
                self._queue.put_nowait(entry)
            except Full:
                self._record_drop()
                return
        else:
            try:
                self._queue.put(entry, block=True, timeout=self.DRF_API_LOGGER_PUT_TIMEOUT)
            except Full:
                self._record_drop()
                return

    def _record_drop(self):
        self._dropped += 1
        if self.DRF_API_LOGGER_DROPPED_WARN_EVERY > 0 and \
                self._dropped % self.DRF_API_LOGGER_DROPPED_WARN_EVERY == 0:
            logger.warning(
                "drf_api_logger: dropped %d log entries due to full queue",
                self._dropped,
            )

    def start_queue_process(self):
        while True:
            try:
                time.sleep(self.DRF_LOGGER_INTERVAL)
                self._start_bulk_insertion()
            except Exception:
                logger.exception("drf_api_logger: unexpected error in flush loop")

    def _start_bulk_insertion(self):
        bulk_item = []
        while not self._queue.empty():
            bulk_item.append(self._queue.get())
        if bulk_item:
            self._insert_into_data_base(bulk_item)

    def _insert_into_data_base(self, bulk_item):
        from django.db import connections

        alias = self.DRF_API_LOGGER_DEFAULT_DATABASE
        for attempt in range(self.DRF_LOGGER_MAX_RETRIES):
            try:
                with transaction.atomic(using=alias):
                    APILogsModel.objects.using(alias).bulk_create(
                        bulk_item,
                        batch_size=self.DRF_LOGGER_BULK_BATCH_SIZE,
                        ignore_conflicts=True,
                    )
                return
            except OperationalError:
                try:
                    connections[alias].close()
                except Exception:
                    logger.exception("drf_api_logger: failed to close stale connection")
                backoff = min(2 ** attempt, 30)
                logger.warning(
                    "drf_api_logger: OperationalError on attempt %d/%d, retrying in %ds",
                    attempt + 1, self.DRF_LOGGER_MAX_RETRIES, backoff,
                )
                time.sleep(backoff)
                continue
            except Exception:
                logger.exception(
                    "drf_api_logger: dropped batch of %d", len(bulk_item),
                )
                return
        logger.error(
            "drf_api_logger: max retries exhausted, dropped batch of %d",
            len(bulk_item),
        )
