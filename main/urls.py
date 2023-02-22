from django.urls import include, path
from main.views import home, trigger_report, get_report

urlpatterns = [
    path('', home, name="home"),
    path('trigger_report/', trigger_report, name="trigger_report"),
    path('get_report/', get_report, name="get_report"),
]