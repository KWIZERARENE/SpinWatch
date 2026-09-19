"""
SpinWatch DRF — Endpoints URL Configuration
Maps each URLpattern to its corresponding APIView class.
All URLs are mounted under /api/ (defined in spinwatch_api/urls.py).
"""

from django.urls import path
from . import views

urlpatterns = [
    # API Root Browser — lists all available endpoints
    path("",                    views.APIRootView.as_view(),        name="api-root"),

    # Point 1 — Telemetry Stream Ingress (GET + POST)
    path("readings/",           views.ReadingsView.as_view(),       name="readings"),

    # Point 1b — Generator Fleet Status
    path("generator/status/",   views.GeneratorStatusView.as_view(), name="generator-status"),

    # Point 2 — Kafka Topic & Partition Inspector
    path("kafka/status/",       views.KafkaStatusView.as_view(),    name="kafka-status"),

    # Point 3a — Kafka Connect Sink Connectors
    path("connect/status/",     views.ConnectStatusView.as_view(),  name="connect-status"),

    # Point 3b — HDFS Raw Store Inspector
    path("hdfs/raw/",           views.HDFSRawView.as_view(),        name="hdfs-raw"),

    # HDFS Available Date Partitions List
    path("hdfs/dates/",         views.HDFSDatesView.as_view(),      name="hdfs-dates"),

    # HDFS Historical Time-Travel Query (?dt=YYYY-MM-DD)
    path("hdfs/history/",       views.HDFSHistoryView.as_view(),    name="hdfs-history"),

    # Point 3c — MySQL Operational Storage (newest first, ORDER BY DESC)
    path("sql/readings/",       views.SQLReadingsView.as_view(),    name="sql-readings"),

    # Point 4a — PySpark MLlib Failure Predictions from HDFS
    path("predictions/",        views.PredictionsView.as_view(),    name="predictions"),

    # Point 4b — PySpark Heat Analytics Insights from HDFS
    path("insights/",           views.InsightsView.as_view(),       name="insights"),

    # Point 5 — Consumer Live Telemetry Buffer
    path("consumer/live/",      views.ConsumerLiveView.as_view(),   name="consumer-live"),
]
