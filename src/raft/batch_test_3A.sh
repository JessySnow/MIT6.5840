#!/bin/bash

for _ in $(seq 1 50); do
  go test -run 3A >> a.txt
done

if grep -q FAIL a.txt; then
  echo "FAIL"
  exit 1
else
  echo "PASS"
  exit 0
fi
