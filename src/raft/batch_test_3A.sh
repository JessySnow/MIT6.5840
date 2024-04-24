#!/bin/bash

for i in $(seq 1 50); do
  go test -run 3A >> a.txt
done

if grep -q FAIL a.txt; then
  echo "FAIL"
else
  echo "PASS"
fi
