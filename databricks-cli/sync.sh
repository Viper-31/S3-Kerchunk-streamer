#!/bin/bash
databricks sync . //Users/johnchewyp1@gmail.com/S3-Kerchunk-Streamer  --profile=Jchew --exclude "*.pyc" --exclude "*venv"--exclude "__pycache__" --watch --interval 5s
