#!/bin/bash

inputJobList="$1"
jobNumber="${2:-0}"
linesPerJob="${3:-0}"
if [ "$linesPerJob" -eq "0" ]; then
    # read all
    while read -r line; do
        jobString="./run_single_fit.py $(echo $line | tr ',' ' ')"
        echo "$jobString"
        eval "$jobString"
    done < $inputJobList
else
    # read only the specified number
    start=$((jobNumber * linesPerJob + 1))
    while read -r line; do
        jobString="./run_single_fit.py $(echo $line | tr ',' ' ')"
        echo "$jobString"
        eval "$jobString"
    done < <(tail -n +$start $inputJobList | head -n $linesPerJob)
fi
