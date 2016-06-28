#!/bin/bash
echo "Source: $1"
echo "Destination: $2"
cat $1 >> $2
echo "Done"
