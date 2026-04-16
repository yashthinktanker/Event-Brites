from django.urls import path

from . import views


urlpatterns = [
    path('', views.home, name='home'),
    path('download/', views.download_csv, name='download_csv'),
    path('single-event/', views.single_event_data, name='single_event_data'),
    path('download/progress/<str:job_id>/', views.download_progress, name='download_progress'),
]
