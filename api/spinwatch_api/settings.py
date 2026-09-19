"""
SpinWatch — Django REST Framework API Browser
Django project settings for the DRF API Browser server (Port 8001).
Mirrors all SpinWatch pipeline inspection endpoints with DRF's
browsable HTML API, serializers, and interactive POST forms.
"""

SECRET_KEY = "spinwatch-drf-dev-secret-key-not-for-production-use"

DEBUG = True

ALLOWED_HOSTS = ["*", "localhost", "127.0.0.1"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "endpoints",
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "spinwatch_api.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
            ],
        },
    },
]

WSGI_APPLICATION = "spinwatch_api.wsgi.application"

# Minimal in-memory SQLite — Django internals only (no SpinWatch data here)
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

# Static files — required for DRF browsable HTML API CSS/JS
STATIC_URL = "/static/"

# Django REST Framework Configuration
REST_FRAMEWORK = {
    # Use DRF's beautiful browsable HTML API as the default renderer
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.BrowsableAPIRenderer",   # HTML browser UI (primary)
        "rest_framework.renderers.JSONRenderer",           # Raw JSON (for Postman / curl)
    ],
    # No authentication — SpinWatch is an internal monitoring tool
    "DEFAULT_AUTHENTICATION_CLASSES": [],
    "DEFAULT_PERMISSION_CLASSES": [],
    # Disable AnonymousUser lookup so django.contrib.auth is not required
    "UNAUTHENTICATED_USER": None,
    "UNAUTHENTICATED_TOKEN": None,
}

# CORS — allow the dashboard (Port 8050) to query this DRF API
CORS_ALLOW_ALL_ORIGINS = True
