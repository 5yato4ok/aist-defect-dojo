"""
Initialization for the AIST integration within DefectDojo.

This package implements the user interface, REST API and Celery
integration for running SAST pipelines. It extends the existing
AISTPipeline functionality with the ability to stop running pipelines,
delete them from the UI, and stream log output to the browser in real time.
"""
from django.apps import AppConfig

class AistConfig(AppConfig):
    name = "dojo.aist"
    verbose_name = "AIST Integration"

    # def ready(self):
    #     # import modules that register Celery signals
    #     from . import celery_signals  # noqa