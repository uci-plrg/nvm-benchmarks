#sys_creat
#sys_open
#sys_read
#sys_write
#sys_close
#sys_lseek
#sys_chdir
#sys_mkdir
#sys_rmdir
#sys_mknod
#sys_fsync
#sys_fdatasync
#sys_link
#sys_unlink
#sys_rename
#sys_fcntl
#sys_fchown
#sys_lchown
#sys_chown
#sys_chmod
#sys_fchmod
#sys_flock

strace -T -ff -o strace -e trace=creat,open,read,write,close,lseek,clone,chdir,mkdir,rmdir,mknod,fsync,fdatasync,link,unlink,rename,fcntl,fchown,lchown,chown,chmod,fchmod,flock -p `pgrep exim`
