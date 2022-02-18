import logging
import threading
LOGGER = logging.getLogger(__name__)


class ILock:
    def __init__(self, name, lock):
        self._name = name
        self._lock = lock()

    def thread(self):
        return threading.current_thread().getName()

    def __enter__(self):
        self._lock.acquire()
        LOGGER.warning("θ;%s;Acq;%s", self._name, self.thread())

    def __exit__(self, exc_type, exc_value, traceback):
        LOGGER.warning("θ;%s;Rel;%s", self._name, self.thread())
        self._lock.release()

    def __call__(self, place: str = None):
        if place:
            LOGGER.warning("θ;%s;Wait;%s;%s", self._name, self.thread(), place)
        return self
