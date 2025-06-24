#!/bin/bash

find temp_output -name "*.tmp.jsonl" -print0 | sort -z | xargs -0 cat -- > "./output/merged_edges.jsonl"