#!/bin/bash

sysbench --test=oltp --oltp-table-size=10000000 --mysql-db=mysql --mysql-user=root\
	--db-driver=mysql --mysql-port=3306 --mysql-socket=/mnt/pmfs/mysql/mysql.sock prepare
