#!/bin/bash

for i in $(seq 1 1000); do
  go test -run 3A >> a.txt
  sleep 1
done

if grep -q FAIL a.txt; then
  echo "FAIL"
else
  echo "PASS"
fi
