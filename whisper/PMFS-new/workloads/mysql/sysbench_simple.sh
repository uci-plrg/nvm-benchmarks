#!/bin/bash

sysbench --test=oltp --oltp-table-size=10000000 --oltp-test-mode=simple --num-threads=8 \
	 --max-requests=1000000 --db-driver=mysql --mysql-port=3306 --mysql-user=root --mysql-db=mysql \
	--mysql-socket=/mnt/pmfs/mysql.sock run

