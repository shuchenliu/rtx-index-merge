#!/bin/bash

find temp_output -name "*.tmp.jsonl" -print0 | sort -z | xargs -0 cat -- > "$1/merged_edges.jsonl"