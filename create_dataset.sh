#!/bin/sh

set -e
date

DATASET="$1"

echo "Start processing for dataset $DATASET"

# Create dataset
if ! bq ls "$DATASET";
then
    bq mk "$DATASET"
else
    echo "Dataset '$DATASET' already exist."
fi

