#!/usr/bin/env bash
# Thin wrapper so the deploy helper is callable as ./deploy.sh on any machine.
exec python3 "$(dirname "$0")/deploy.py" "$@"
