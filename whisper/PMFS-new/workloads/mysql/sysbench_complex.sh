#!/bin/bash

sysbench --test=oltp --oltp-table-size=10000000 --oltp-test-mode=complex --mysql-db=mysql \
	 --max-time=60 --max-requests=10000000 --num-threads=4 --db-driver=mysql --mysql-port=3306 --mysql-user=root \
	--mysql-socket=/mnt/pmfs/mysql/mysql.sock run

