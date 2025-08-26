#!/bin/bash
# Upgrade pip first to get pre-built wheels
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Start your app (example for Flask)
gunicorn app:app --bind 0.0.0.0:$PORT