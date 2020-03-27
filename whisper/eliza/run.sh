
#!/bin/bash

# Equal message sizes
time pypy main.py -f gz/exim-27.gz -y ftrace -w 4
time pypy main.py -f gz/exim-28.gz -y ftrace -w 4
time pypy main.py -f gz/exim-17.gz -y ftrace -w 4
time pypy main.py -f gz/exim-29.gz -y ftrace -w 4
time pypy main.py -f gz/exim-30.gz -y ftrace -w 4
time pypy main.py -f gz/exim-31.gz -y ftrace -w 4
time pypy main.py -f gz/exim-32.gz -y ftrace -w 4
# Unequal message size 0-max
time pypy main.py -f gz/exim-40.gz -y ftrace -w 4
time pypy main.py -f gz/exim-39.gz -y ftrace -w 4
time pypy main.py -f gz/exim-38.gz -y ftrace -w 4
time pypy main.py -f gz/exim-37.gz -y ftrace -w 4
time pypy main.py -f gz/exim-36.gz -y ftrace -w 4
# NFS
time pypy main.py -f gz/nfs-20.gz -y ftrace -w 4
# DB
time pypy main.py -f gz/mysql-00.gz -y ftrace -w 4

