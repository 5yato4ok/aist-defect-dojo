from django.urls import path
from . import views

app_name = 'dojo_aist'

urlpatterns = [
    path('start', views.start_pipeline, name='start_pipeline'),
    path('pipeline/<str:id>', views.pipeline_detail, name='pipeline_detail'),
]
