#!/bin/bash
#./postal -m 100 -M 100 -t 1 -c 1 -r 1 localhost user-list-filename  
timeout 2m postal -m 100 -M 100 -t 8 -c 2 -r 1000 localhost user-list-filename  
