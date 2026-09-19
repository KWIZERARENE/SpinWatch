#!/usr/bin/env python
"""
SpinWatch DRF API Browser — Django Management CLI Entry Point
Run with:   python api/manage.py runserver 8001
"""

import os
import sys


def main():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "spinwatch_api.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Django is not installed. Run: pip install django djangorestframework django-cors-headers"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    # Ensure the api/ directory is in sys.path so spinwatch_api is importable
    api_dir = os.path.dirname(os.path.abspath(__file__))
    if api_dir not in sys.path:
        sys.path.insert(0, api_dir)
    main()
