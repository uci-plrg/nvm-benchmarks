#!/bin/bash
mkdir /mnt/pmfs/mysql
bin/mysqld --defaults-file=support-files/pmfs_mysql.cnf --initialize-insecure

