from typing import Callable
import functools
import logging


logger = logging.getLogger(__name__)


def signal_handler(signum, frame):
    raise ProgramKilled


class InterfaceError(Exception):
    """Raised when communication with the management interface fails"""
    pass


class ProgramKilled(Exception):
    pass


def rethrow_interface_error(f: Callable):
    @functools.wraps(f)
    def rethrow_interface_error_wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f'Exception in {f.__name__}: {e}')
            raise InterfaceError()
    return rethrow_interface_error_wrapper
