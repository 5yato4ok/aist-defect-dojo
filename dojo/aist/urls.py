from django.urls import path
from . import views
from . import views_push_to_ai

app_name = "dojo_aist"
urlpatterns = [
    path('start', views.start_pipeline, name='start_pipeline'),
    path("pipelines/<str:id>/", views.pipeline_detail, name="pipeline_detail"),
    path("pipelines/<str:id>/stop/", views.stop_pipeline_view, name="pipeline_stop"),
    # JSON endpoints used by the new UI
    path('products/<int:product_id>/analyzers.json', views_push_to_ai.product_analyzers_json, name='product_analyzers_json'),
    path('findings/search.json', views_push_to_ai.search_findings_json, name='search_findings_json'),

    path('pipelines/<str:pipeline_id>/send_request_to_ai', views_push_to_ai.send_request_to_ai, name='send_request_to_ai'),

    path("pipelines/<str:id>/delete/", views.delete_pipeline_view, name="pipeline_delete"),
    path("pipelines/<str:id>/logs/stream/", views.stream_logs_sse, name="pipeline_logs_stream"),
    path("pipelines/<str:id>/progress/deduplication", views.deduplication_progress_json, name="deduplication_progress"),
    path("pipelines/<str:id>/callback/", views.pipeline_callback, name="pipeline_callback"),
    path("pipeline/<str:id>/status/stream/", views.pipeline_status_stream, name="pipeline_status_stream"),
    path("aist/default-analyzers/", views.aist_default_analyzers, name="aist_default_analyzers"),
    path("pipelines/", views.pipeline_list, name="pipeline_list"),
    path("pipelines/<str:id>/set_status_push_to_ai", views.pipeline_set_status, name="pipeline_set_status"), #TODO: make generic
    path("projects/<int:pk>/meta.json", views.project_meta, name="aist_project_meta"),
    path("pipeline/<str:id>/progress/enrichment", views.pipeline_enrich_progress_sse, name="pipeline_enrich_progress")
]
