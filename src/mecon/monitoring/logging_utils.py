import logging
import time
from functools import wraps


def codeflow_log_wrapper(tags=''):
    def decorator(_func):
        # https://flask.palletsprojects.com/en/2.3.x/patterns/viewdecorators/
        @wraps(_func)
        def wrapper(*args, **kwargs):  # TODO indent function call
            _funct_name = f"{_func.__module__}.{_func.__qualname__}"
            logging.debug(
                f"{_funct_name} started... #codeflow#start#{_func.__qualname__}{tags}")  # TODO remove function_start log
            time_started = time.time()
            try:
                res = _func(*args, **kwargs)
                exec_dur = time.time() - time_started
                logging.debug(
                    f"{_funct_name} finished.  ({exec_dur=} seconds) #codeflow#end#{_func.__qualname__}{tags}")
                return res
            except Exception as e:
                logging.error(f"{_funct_name} raised {e}! #codeflow#error#{_func.__qualname__}{tags}")
                raise

        return wrapper

    return decorator
