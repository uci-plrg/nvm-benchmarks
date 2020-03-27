# PMFS

PMFS is a file system for persistent memory, developed by Intel.
For more details about PMFS, please check the git repository:

https://github.com/linux-pmfs/pmfs

WHISPER provides PMFS as a Linux Kernel Module (LKM) for v4.3 only.

## Current limitations

* PMFS only works on x86-64 kernels.
* PMFS does not currently support extended attributes or ACL.
* PMFS requires the underlying block device to support DAX (Direct Access) feature.
* This project cuts some features of the original PMFS, such as memory protection and huge mmap support. If you need these features, please turn to the original PMFS.

## First, enable PM support in Linux.

* Download linux-4.3 from kernel.org.
* Use "make menuconfig" to edit kernel config.
* In the kernel config file:
```
	CONFIG_BLK_DEV_RAM_DAX=y
	CONFIG_FS_DAX=y
	CONFIG_X86_PMEM_LEGACY=y
	CONFIG_LIBNVDIMM=y
	CONFIG_BLK_DEV_PMEM=m
	CONFIG_ARCH_HAS_PMEM_API=y
```
* Compile and install the kernel.

* Edit /etc/default/grub or an equivalent file that generates grub menu
  in your system. Use the memmap kernel boot parameter to reserve a region
  of memory to act as PM. Eg., to reserve 4G of memory starting at 2GB mark
```
	memmap=4G!2G
```
Update grub using "grub2-mkconfig -o your-grub.cfg"

*  Refer here for more details on how to expose PM:

	https://nvdimm.wiki.kernel.org/

*  Reboot. On success, you should see PM as a device named /dev/pmem0.

## To compile and run PMFS as LKM in Linux 4.3:

*  Install kernel headers for development. Use appropriate package manager for 
   your system. For Aptitude manager, use the following command:
```
	apt-get install linux-headers-4.3
```
*  Get PMFS source code and change to PMFS source directory.
```
	$ git clone https://github.com/snalli/PMFS-new.git
	$ cd PMFS-new
	$ make 
```
## To compile tracing framework for PM accesses:
```
	$ make CPPFLAGS="-D__TRACE__"
```
## To run:
```	
	$ mkdir /mnt/pmfs
	$ insmod pmfs.ko measure_timing=0
	$ mount -t pmfs -o init,tracemask=0,jsize=256M /dev/pmem0 /mnt/pmfs
```
To enable tracing, set tracemask to 1. You may vary journal size using jsize 
as per your requirements.

*  On success, you should see PMFS mounted at /mnt/pmfs.
```
	$ mount | grep pmfs
```
*  For more details, use the Makefile or README.

## PMFS Workloads

WHISPER includes 3 filesystem workloads: Network File Server (NFS), Mail server (Exim)
and Database server (MySQL) that are partially or entirely in PMFS/workloads/ folder.

## To compile and run NFS in Linux:

*  In the kernel config file:
```
	CONFIG_NFS_FS=y
	CONFIG_NFS_V3=y
	CONFIG_NFSD=y
	CONFIG_NFSD_V3=y
```
These are the minimum configuration parameters to compile NFS in Linux.  You
may enable more parameters as per your requirement.  For more details, refer to
fs/Kconfig, fs/nfs/Kconfig, fs/nfsd/Kconfig in the Linux kernel source tree.
Compile, re-install and reboot.

*  Mount PMFS and inform NFS using /etc/exports file. In exports, copy and paste:
```
	/mnt/pmfs	localhost.localdomain(rw,sync,wdelay,nohide,nocrossmnt,secure,	\
			no_root_squash,no_all_squash,no_subtree_check,secure_locks,acl,	\
			no_pnfs,fsid=10,anonuid=65534,anongid=65534,sec=sys,rw,secure,	\
			no_root_squash,no_all_squash)
```
* Then export it using:
```
	$ exportfs -a
	$ exportfs 		[To check exported mountpoints]
```
* Start NFS server:
```	
	$ service nfs start
	$ rpc.nfsd 8		[This starts 8 server threads. May change as per needs]
	$ service nfslock start
	$ pgrep nfs		[To verify]
```
*  The client should be started after the server. 
   Start NFS client:
```	
	$ mount.nfs -s localhost:/mnt/pmfs /mnt/nfs -wn -o retrans=3,soft,noac,		\
				lookupcache=none,rsize=4096,wsize=4096,proto=udp,timeo=7
```
*  On success, you should see NFS mounted on the client side.
```	
	$ mount | grep nfs
```
At this point, both server and client have been setup to provide access to PMFS.
You may test this by creating a file in /mnt/nfs from the client side and verify
that a file has been created in PMFS on the server.
```
	$ strace touch /mnt/nfs/foo		[Create at the client. May wait.]
	$ ls /mnt/pmfs				[Verify at the server]
```
*  Pre-create the fileset for filebench on the NFS server side using fileserver-asplos.f in workloads/filsrv.
```
	$ ./filebench -f fileserver-asplos.f	[Pre-create at the server]
```
*  Launch filebench on the NFS client side using nfs-asplos.f in workloads/filsrv.
```
	$ ./filebench -f nfs-asplos.f		[Access at the client]
```

## To compile and run Exim mailserver:

*  Get exim-4.86 from workloads/mailsrv. Use the README for compilation
   or simply do the following.

	In the exim configuration file exim-4.86/Local/Makefile
	```
	change BIN_DIRECTORY
	change CONFIGURE_FILE
	change EXIM_USER
	```
In your system, create folders /mnt/pmfs/exim, /mnt/pmfs/exim/spool and /mnt/pmfs/exim/spool/mail. 

May need to change permissions on these folders so that exim can store e-mails here.

### This creates a mail directory in PM under /mnt/pmfs/exim/spool/mail/

Next,
```
	$ make
	$ make install	
```
This will install exim in the BIN_DIRECTORY

*  Create user accounts for exim users using "useradd" and user-list-filename in mailsrv/,
   only as a guide. Ensure they all belong to the same group that has r/w permissions
   to the spool and mail folders created in Step 1.

*  To run exim
```
	$ ./exim -bd &
	$ pgrep exim						[Verify]
```
*  To install and run postal for 2 minutes, 
```
	$ apt-get install postal
	$ postal -h
	$ timeout 2m postal -m 100 -M 100 -t 8 -c 2 -r 1000 localhost user-list-filename  
```
*  On success, you should files with each user's name created in step 2 and some 
   emails in them.

## To compile and run MySQL:

*  Get MySQL 5.7.6 from https://www.mysql.com/.
*  Follow its instructions to compile.
*  To initialize DB server:
```	
	$ cp PMFS-new/workloads/mysql/* your-mysql-dir
	$ cd your-mysql-dir
	$ cp pmfs_mysql.conf support-files/
```
Change base dir in the conf file to your-mysql-dir

Next,
```
	$ ./mysql_init.sh
```
### This creates a database in PM under /mnt/pmfs/mysql/

*  To run DB server:
```
	$ ./mysql_run.sh
```
*  To initialize Sysbench, a workload for MySQL :
```
	$ ./sysbench_init.sh
```
*  To run Sysbench workloads :
```
	$ ./sysbench_simple.sh
	$ ./sysbench_complex.sh
```
