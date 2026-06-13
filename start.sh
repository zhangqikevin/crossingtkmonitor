#!/bin/bash
# 启动 TikTok Live Monitor
cd "$(dirname "$0")"
exec ./venv/bin/python server.py
