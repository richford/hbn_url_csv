#! /bin/bash

SUBLISTFILE=$1
ALLSUBSCSV=$2
OUTPUTDIR=$3

for subid in $(cat $SUBLISTFILE); do
    /usr/bin/grep "$subid" "$ALLSUBSCSV" > "$OUTPUTDIR/$subid.csv"
done
