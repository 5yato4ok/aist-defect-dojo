# dojo/aist/celery_signals.py
from celery.signals import worker_process_init
from .logging_transport import install_global_redis_log_handler

@worker_process_init.connect
def setup_logging_on_worker(**kwargs):
    pass
    # install handler once per worker process
    #install_global_redis_log_handler()
