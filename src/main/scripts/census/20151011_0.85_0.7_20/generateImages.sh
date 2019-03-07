#!/bin/sh

grayScale=${1:-0}
interpolation=${2:-"bicubic"}

echo $grayScale
echo $interpolation
for i in `ls *.mat`
do
    echo $i 
    python2.7 imageFromMatrix.py "$i" $grayScale $interpolation
done

