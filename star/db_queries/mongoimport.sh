#!/usr/bin/env bash

ls -1 *.json | while read col; do
    mongoimport -d star_raw -c raw_staging --file $col;
done
