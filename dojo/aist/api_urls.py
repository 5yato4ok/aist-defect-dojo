from .api import AISTProjectAPI, PipelineStartAPI, PipelineAPI, PipelineListAPI
from django.urls import path

app_name = "dojo_aist"
urlpatterns = [
    path("projects/", AISTProjectAPI.as_view(), name="project_list"),
    path("pipelines/start", PipelineStartAPI.as_view(), name="pipeline_start"),
    path("pipelines/<str:id>", PipelineAPI.as_view(), name="pipeline_status"),
    path("pipelines/", PipelineListAPI.as_view(), name="pipelines"),
]