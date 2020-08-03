#! /bin/bash

for fname in $@; do
    gsed -i 's/s3:\/\//http:\/\/s3.amazonaws.com\//g' $fname
done
