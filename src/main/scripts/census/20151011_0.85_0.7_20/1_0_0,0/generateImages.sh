#!/bin/sh

grayScale=${1:-0}
interpolation=${2:-"bicubic"}

echo $grayScale
echo $interpolation
for i in `ls Component*.mat`
do
    echo $i 
    python cpImageFromMatrix.py "$i" 
done

