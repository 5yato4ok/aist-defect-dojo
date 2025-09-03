from django.urls import path
from .views import pipeline_detail, stop_pipeline_view, delete_pipeline_view, stream_logs_sse, start_pipeline,  pipeline_progress_json

app_name = "dojo_aist"
urlpatterns = [
    path('start', start_pipeline, name='start_pipeline'),
    path("pipelines/<str:id>/", pipeline_detail, name="pipeline_detail"),
    path("pipelines/<str:id>/stop/", stop_pipeline_view, name="pipeline_stop"),
    path("pipelines/<str:id>/delete/", delete_pipeline_view, name="pipeline_delete"),
    path("pipelines/<str:id>/logs/stream/", stream_logs_sse, name="pipeline_logs_stream"),
    path("pipelines/<str:id>/progress/", pipeline_progress_json, name="pipeline_progress"),
]
