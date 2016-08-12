#!/bin/bash

set -e

# usage
cmdname=`basename $0`
function usage()
{
  echo "Usage: ${cmdname} host user password db table bucket" 1>&2
}

if [ $# -ne 6 ]; then
  usage
  exit 1
fi

HOST="$1"
USER="$2"
PASSWORD="$3"

DB="$4"
TABLE="$5"
COMB="$DB"_"$TABLE"

BUCKET="$6"

# directory
[ -d format ] || mkdir format
[ -d sql ] || mkdir sql
[ -e data ] || mkdir data

date
echo "Create schema files"
./create_schema.py -h$HOST -p$PASSWORD -u$USER $DB --table $TABLE

# Dump to TSV
date
echo "Start bump from mysql"
mysql -h$HOST -p$PASSWORD -u$USER $DB -e "`cat sql/$COMB.sql`" | gzip > data/$COMB.tsv.gz
ls -lh data/$COMB.tsv.gz

./create_dataset.sh $DB


date
echo "Upload data to gs"
gsutil cp data/$COMB.tsv.gz gs://$BUCKET/$COMB.tsv.gz

date
echo "Load data to bq from gs"
bq load --field_delimiter="\t" --skip_leading_rows=1 $DB.$TABLE gs://$BUCKET/$COMB.tsv.gz format/$COMB.json

date
echo "Finished"
