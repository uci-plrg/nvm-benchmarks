# echo-keyvalue
A scalable and efficient key-value store for non-volatile memory (NVM). 

Exploring storage class memory with key value stores. 
Katelin A. Bailey, Peter Hornyack, Luis Ceze, Steven D. Gribble, and Henry M. Levy
Workshop on Interactions of NVM/FLASH with Operating Systems and Workloads (INFLOW '13)

# To build
~~~
    $ cd echo/src
    $ ./build
~~~

# To run :

~~~
    $ cd echo/src
    $ ./run --small                             [can pass --med or --large for bigger workloads]
~~~

Echo will create a persistent heap in /dev/shm defined by the macro PERSISTENT_HEAP.

To collect the trace of accesses to persistent memory,
make sure you have debugfs mounted in Linux.

# To run with tracing enabled :
~~~
    $ mount | grep debugfs
    debugfs on /sys/kernel/debug type debugfs (rw,relatime)
    
    $ sudo cd /sys/kernel/debug
    $ sudo echo 128000 > buffer_size_kb         [Allocate enough buffer to hold traces]
    $ cat trace_pipe                            [Redirect output of pipe to any file for storage]
    
    Go back to echo/:
    
    $ cd echo/src
    $ sudo ./run --small --trace                [Need to be root to collect trace]
~~~
