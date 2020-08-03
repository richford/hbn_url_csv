#! /bin/bash

ALLSUBSCSV=$1
OUTPUTDIR=$2

for fname in $2/*.csv; do
    head -n1 $1 | cat - $fname | sponge $fname
done
