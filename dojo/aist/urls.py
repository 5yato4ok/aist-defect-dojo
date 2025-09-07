from django.urls import path
from .views import pipeline_detail, stop_pipeline_view, delete_pipeline_view, stream_logs_sse, start_pipeline, \
    pipeline_progress_json, pipeline_callback, pipeline_status_stream, aist_default_analyzers,pipeline_list, push_to_ai, project_meta

app_name = "dojo_aist"
urlpatterns = [
    path('start', start_pipeline, name='start_pipeline'),
    path("pipelines/<str:id>/", pipeline_detail, name="pipeline_detail"),
    path("pipelines/<str:id>/stop/", stop_pipeline_view, name="pipeline_stop"),
    path("pipelines/<str:id>/push_to_ai/", push_to_ai, name="push_to_ai"),
    path("pipelines/<str:id>/delete/", delete_pipeline_view, name="pipeline_delete"),
    path("pipelines/<str:id>/logs/stream/", stream_logs_sse, name="pipeline_logs_stream"),
    path("pipelines/<str:id>/progress/", pipeline_progress_json, name="pipeline_progress"),
    path("pipelines/<str:id>/callback/", pipeline_callback, name="pipeline_callback"),
    path("pipeline/<str:id>/status/stream/", pipeline_status_stream, name="pipeline_status_stream"),
    path("aist/default-analyzers/", aist_default_analyzers, name="aist_default_analyzers"),
    path("pipelines/", pipeline_list, name="pipeline_list"),
    path("projects/<int:pk>/meta.json", project_meta, name="aist_project_meta")
]
