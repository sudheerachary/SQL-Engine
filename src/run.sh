#!/bin/bash
for dir in "$@"
do
    python query_processor.py "$dir"
done