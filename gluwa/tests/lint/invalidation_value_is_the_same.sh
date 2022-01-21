#!/bin/bash

set -e

# Test for CRB-303
unique_values=$(grep " InValidation =" validator/* -R | tr -d ' ,' | cut -f2 -d: | sort | uniq | wc -l)
if [ "$unique_values" != "1" ]; then
    echo "FAIL: Field InValidation contains different values"
    exit 1
fi
