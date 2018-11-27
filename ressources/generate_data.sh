#!/bin/bash

length=$1;

for ((i=0;z<length;z++))
do
	 printf "%d, " $((RANDOM%$1))
done