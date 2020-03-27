/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: nil
 * vi: set shiftwidth=2 tabstop=2 expandtab:
 * :indentSize=2:tabSize=2:noTabs=true:
 *
 * Katelin Bailey & Peter Hornyack
 * 8/28/2011
 */

//#define CASSANDRA_INSTALLED


#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <sched.h>
#include <getopt.h>

#include "kp_macros.h"
#include "kp_common.h"
#include "../include/kp_kv_local.h"
#include "../include/kp_kv_master.h"
#include "leveldb_client.h"
#include "PIN_Hooks.h"
#ifdef CASSANDRA_INSTALLED
#include "cassandra_client.h"
#endif

bool do_conflict_detection = true;

/* Switch on which keyvalue store to use */
#define USE_KP_KV_STORE 0
#define USE_CASSANDRA 1
#define USE_LEVELDB 2

#define USE_DISK 0
#define USE_SSD 1
#define USE_RAMDISK 2

#define DISK_PATH  "/var/tmp/leveldb_client_name"
#define SSD_PATH "/scratch/nvm/leveldb_client_name"
#define RAMDISK_PATH "/mnt/ramdisk/leveldb_client_name"

/* define PRINT_CPUS to print out which CPU each thread is pinned to. */
//#define PRINT_CPUS

#define PERSISTENT_LDB true
int WHICH_STORAGE_PLATFORM;
int WHICH_KEY_VALUE_STORE;

/* Consistency mode determines
 * where gets go (master or local)
 * how often we merge (wrt puts)
 */
//#define MASTER_CONSISTENCY_MODE MODE_WEAK
#define MASTER_CONSISTENCY_MODE MODE_SNAPSHOT
//#define MASTER_CONSISTENCY_MODE MODE_SEQUENTIAL

/* These two parameters define whether or not we perform extra non-volatile
 * memory steps for the local stores and the master store: */
bool use_nvm_local  = false;
bool use_nvm_master = true;

/* These two constants define the range of CPUs that the worker threads
 * will be placed on: [CPU_OFFSET, CPU_OFFSET + NUM_CPUS - 1]. If
 * the high end of that range is greater than the number of CPUs on
 * the system, who knows what will happen.
 */
int NUM_CPUS=2;
#define CPU_OFFSET 0
#define MAX_THREADS 256

#define K_DEBUG 0      //Print Debug messages
#define MULT_FACTOR 3  //For cheap operations, # of extra times to iterate
#define MT_EXTEND 30   //To ensure the threads last a while, do extra iters
#define BENCH_EXTEND 5   //To ensure the threads last a while, do extra iters
#define USE_OTHER_TIMERS 0
#define RET_STRING_LEN 64

/* define this to debug the operations performed by the LATENCY and
 * WORKER threads: */
//#define EVAL_DEBUG
#ifdef EVAL_DEBUG
#define eval_debug(f, a...)  do { \
      fprintf(stdout, "%lu: %s: " f, pthread_self(), __func__, ##a); \
      fflush(stdout); \
      } while(0)
#else
#define eval_debug(f, a...) do { ; } while(0)
#endif

typedef struct random_ints_ {
  int *array;
  unsigned int count;
  unsigned int idx;
} random_ints;

/* Structure to push arguments to the worker */
typedef struct benchmark_args_struct {
	cpu_set_t cpu_set;
	void *master;
  int num_threads;
  int starting_ops;
  pthread_cond_t *bench_cond;
  pthread_mutex_t *bench_mutex;
  bool slam_local;
  bool split_keys;
  int my_id;
  bool do_measure;
  random_ints *ints;
} benchmark_thread_args;

typedef struct kp_timers_struct {
  /* Struct timespec returns timing information at second and nanosecond
   * granularity. On host burrard, clock_getres() confirms that the
   * resolution of these timers is indeed 1 nanosecond.
   */
  struct timespec realtime_start;      //clock_gettime(CLOCK_REALTIME)
  struct timespec realtime_stop;
  struct timespec monotonic_start;     //clock_gettime(CLOCK_MONOTONIC)
  struct timespec monotonic_stop;
  struct timespec proc_cputime_start;  //clock_gettime(CLOCK_PROCESS_CPUTIME_ID)
  struct timespec proc_cputime_stop;
} kp_timers;


/* Parameters that vary the evaluation */
unsigned int max_kv_size;        //global; will be set in main()
size_t local_expected_max_keys;  //set in main() as a function of max_kv_size
int iterations = 100000;            // Number of iterations to get average
int merge_every = 1000;             // Number of operations before we merge
int key_size = 8;                   // Uniform size for all keys
int value_size = 16;                // Uniform size for all values
float put_probability = 0;          // Probability that next request is a put
float update_probability = .7;       // Prob. that next put is to existing val

char *log_file_name = "kpvm_logged_errors.out";
FILE *log_file;


/* Keep track of some keys so we can run random workload */
int num_keys = 0;                    // For an instance, num unique keys in set
int thpt_numputs = 0;
int thpt_numgets = 0;
int thpt_numdels = 0;

/* Do some block allocations at initialization */
void **dest_values;                  // Generate a gazillion potential values 
//void **temp_values;                  // Generate a gazillion potential values 
char **temp_keys;                    // Generate a gazillion potential keys 

void *temp_value;
void *dest_value;
unsigned int random_int_count = 0;

/* Timing Functions */
void start_timing(kp_timers *timers);
void stop_timing(kp_timers *timers);
void print_timing(const char *prefix, kp_timers *timers, int divisor);

/* Interactions with the stores, as generic functions*/
void *generic_store_create(void);
void *generic_worker_create(void *master_store);
void generic_store_destroy(void *store);
void generic_worker_destroy(void *store);
void *generic_trans_start(void *store);
int generic_trans_end(void *store,void *trans);
int generic_get(void *store, char *key, char **value, size_t* size);
int generic_put(void *store, void *trans, char *key, char *value, size_t size);
int generic_delete(void *store, void *trans, char *key);

/* Randomizing functions */
void reset_counts(void);
int biased_update_coin(random_ints *ints);
int biased_put_get_coin(random_ints *ints);
int pick_random_key(int input_num_keys, random_ints *ints);
int create_random_key(int size, char *key);
int create_random_value(int size, void *value);
int random_ints_create(random_ints **ints, unsigned int count);
int random_ints_next(random_ints *ints);
void random_ints_destroy(random_ints *ints);

/* Workload, microbenchmark, and request generation functions */
int random_on_single_key(void *store, void *trans, int i, char* key,
    random_ints *ints);
int random_on_partitioned_keys(void *store, void *trans, int i, 
                               int offset, int *partition_count,
                               random_ints *ints);
int create_random_request(void *store, void *trans, int i, random_ints *ints);
int workload_evaluation(int num_iterations, void *store, random_ints *ints);
int latency_evaluation(int num_iterations, void* store);
int split_latency_evaluation(int num_iterations, void* store);
 int local_keyvalue_size(int num_iterations, kp_kv_local *kv);

 /* Threaded workload */
 void *worker_thread_entrypoint(void *arg);
 void ramp_up_threads(int num_threads);

 /* Threaded macrobenchmarks */
 void *fixed_benchmark_entrypoint(void *arg);
 void fixed_generic_benchmark(int num_threads, char* bench_string,
                              bool slam_local, bool split_keys,
                              bool measurement_thread);
 void fixed_global_benchmark(int num_threads);
 void fixed_local_benchmark(int num_threads);
 void fixed_read_benchmark(int num_threads);
 void fixed_write_benchmark(int num_threads);
 void fixed_update_benchmark(int num_threads);
 void fixed_with_measurement_thread(int num_threads);
 /* Wrappers for single-threaded evaluations */
 void *little_latency_wrapper(void *arg);
 void *little_workload_wrapper(void *arg);
 void *little_kvsize_wrapper(void *arg);
 void single_worker_test(void);

 /* utilities*/
 void set_process_affinity();
 void usage(char *progname);
 void parse_arguments(int argc, char *argv[], int *num_threads,
                      bool *pause_before_start, int *delay, 
                      bool *base);


 /* Starts various timers.
  */
 void start_timing(kp_timers *timers) {
   if (! USE_OTHER_TIMERS)
     return;

   int ret;

   ret = clock_gettime(CLOCK_REALTIME, &(timers->realtime_start));
   if (ret != 0) {
     printf("ERROR: startclock_gettime(CLOCK_REALTIME) returned %d\n", ret);
   }

   ret = clock_gettime(CLOCK_MONOTONIC, &(timers->monotonic_start));
   if (ret != 0) {
     printf("ERROR: startclock_gettime(CLOCK_MONOTONIC) returned %d\n", ret);
   }

   ret = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &(timers->proc_cputime_start));
   if (ret != 0) {
     printf("ERROR: startclock_gettime(CLOCK_PROCESS_CPUTIME_ID) returned %d\n",
         ret);
   }
 }

 /* Stops various timers. The kp_timers pointer that is passed to
  * this function must have been passed to start_timing() before
  * it is used here, or undefined behavior may result.
  */
 void stop_timing(kp_timers *timers) {
   if (! USE_OTHER_TIMERS)
     return;
   int ret;

   ret = clock_gettime(CLOCK_REALTIME, &(timers->realtime_stop));
   if (ret != 0) {
     printf("ERROR: stop clock_gettime(CLOCK_REALTIME) returned %d\n", ret);
   }

   ret = clock_gettime(CLOCK_MONOTONIC, &(timers->monotonic_stop));
   if (ret != 0) {
     printf("ERROR: stop clock_gettime(CLOCK_MONOTONIC) returned %d\n", ret);
   }

   ret = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &(timers->proc_cputime_stop));
   if (ret != 0) {
     printf("ERROR: stop clock_gettime(CLOCK_PROCESS_CPUTIME_ID) returned %d\n",
         ret);
   }

 }

 /* Subtracts struct timespec *b from struct timespec *a. The tv_nsec member
  * of struct timespec is a long, so we know it's signed and we can check
  * if the difference is negative in this code.
  * http://stackoverflow.com/questions/1858050/how-do-i-compare-two-timestamps-in-c
  * http://www.gnu.org/s/hello/manual/libc/Elapsed-Time.html
  */
 #define ts_subtract(a, b, result) \
   do { \
     (result)->tv_sec = (a)->tv_sec - (b)->tv_sec; \
     (result)->tv_nsec = (a)->tv_nsec - (b)->tv_nsec; \
     if ((result)->tv_nsec < 0) { \
       (result)->tv_sec -= 1; \
       (result)->tv_nsec += 1000000000; \
     } \
   } while (0)


 /* Prints out timing information. The kp_timers pointer that is passed
  * to this function must have first been passed to start_timing() and
  * stop_timing(), or undefined behavior may result. The prefix argument
  * will be used to identify the operations whose timing information
  * we are printing here. divisor is the number of iterations, so that
  * average operation latency can be computed.
  */
 void print_timing(const char *prefix, kp_timers *timers, int divisor) {
   if (! USE_OTHER_TIMERS)
     return;

   struct timespec ts_diff;
   unsigned long total_usec;
   float avg_usec;

   ts_subtract(&(timers->realtime_stop), &(timers->realtime_start), &ts_diff);
   total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
   avg_usec = (float)total_usec / (float)divisor;
   printf("%s: \n\t CLOCK_REALTIME \t %lu usec, average cost %f usec\n",
       prefix, total_usec, avg_usec);

   ts_subtract(&(timers->monotonic_stop), &(timers->monotonic_start), &ts_diff);
   total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
   avg_usec = (float)total_usec / (float)divisor;
   printf("\t CLOCK_MONOTONIC \t %lu usec, average cost %f usec\n",
       total_usec, avg_usec);

   ts_subtract(&(timers->proc_cputime_stop), &(timers->proc_cputime_start), 
               &ts_diff);
   total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
   avg_usec = (float)total_usec / (float)divisor;
   printf("\t CLOCK_PROC_CPUTIME_ID \t %lu usec, average cost %f usec\n",
       total_usec, avg_usec);

   printf("\n");
 }

 void *generic_store_create(void){
   int rc = 0;
   kp_kv_master *master;
   leveldb *db = NULL;
   void *store;
   unsigned long tid = pthread_self();
   #ifdef CASSANDRA_INSTALLED
   cassandra_node *node = NULL;
   #endif


   switch ( WHICH_KEY_VALUE_STORE ) {
   case USE_KP_KV_STORE:
     /* Use the max_kv_size as the size of the hash table: */
     rc = kp_kv_master_create(&master, MASTER_CONSISTENCY_MODE,
         max_kv_size, do_conflict_detection, use_nvm_master);
     if(rc != 0){
       kp_die("thread_%lu: kp_kv_master_create() returned error=%d\n", tid, rc);
       return NULL;
     }
     store = (void *) master;
     break;

   #ifdef CASSANDRA_INSTALLED
   case USE_CASSANDRA:
     rc =  cassandra_start(&node, CASSANDRA_PORT_DEFAULT);
     if(rc != 0){
       kp_die("thread_%lu: cassandra_start() returned error=%d\n", tid, rc);
       return NULL;
     }
     store = (void *) node;
     break;
   #endif

   case USE_LEVELDB:
     switch (WHICH_STORAGE_PLATFORM){
     case USE_DISK:
       rc = leveldb_client_start(&db, DISK_PATH, 0, false, PERSISTENT_LDB);
       break;
     case USE_SSD:
       rc = leveldb_client_start(&db, SSD_PATH, 0, false, PERSISTENT_LDB);
       break;
     case USE_RAMDISK:
       rc = leveldb_client_start(&db, RAMDISK_PATH, 0, false, PERSISTENT_LDB);
       break;
     default:
       kp_die("Request to use unknown Storage Platrofm\n");
     break;
     }
     if (rc != 0){
       kp_die("thread_%lu: leveldb_client_start() returned error=%d\n", tid,rc);
       return NULL;
     }
     store = (void *) db;
     break;
   default:
     kp_die("Request to use unknown KeyValueStore\n");
     break;
   }
   return store;

 }

 void *generic_worker_create(void *master_store) {
   int rc = 0;
   kp_kv_master *master;
   kp_kv_local *local;
   void *store;
   unsigned long tid = pthread_self();
   #ifdef CASSANDRA_INSTALLED
   cassandra_node *node;
   #endif

 #ifdef KP_ASSERT
   if (local_expected_max_keys <= 0) {
     kp_die("unexpected local_expected_max_keys value: %u\n",
         local_expected_max_keys);
   }
 #endif

   switch ( WHICH_KEY_VALUE_STORE ) {
   case USE_KP_KV_STORE:
     master = (kp_kv_master*) master_store;
     rc = kp_kv_local_create(master, &local, local_expected_max_keys, use_nvm_local);
     if(rc != 0){
       kp_die("thread_%lu: kp_kv_local_create() returned error=%d\n", tid, rc);
       return NULL;
     }
     store = (void *) local;
     break;

   #ifdef CASSANDRA_INSTALLED
   case USE_CASSANDRA:
     rc =  cassandra_worker_start(&node, CASSANDRA_PORT_DEFAULT);
     if(rc != 0){
       kp_die("thread_%lu: cassandra_start() returned error=%d\n", tid, rc);
       return NULL;
     }
     store = (void *) node;
     break;
   #endif

   case USE_LEVELDB:
     /* Since we send requests to the master store for leveldb */
     store = (void *) master_store;
     break;
   default:
     kp_die("Request to use unknown KeyValueStore\n");
     break;
   }
   return store;
 }

 void generic_store_destroy(void *store){
   int rc = 0;
   kp_kv_master *master;
   leveldb *db;
   unsigned long tid = pthread_self();
   #ifdef CASSANDRA_INSTALLED
   cassandra_node *node;
   #endif

   switch (WHICH_KEY_VALUE_STORE){
   case USE_KP_KV_STORE:
     master = (kp_kv_master *) store;
     rc = kp_kv_master_destroy(master);
     if (rc != 0){
       kp_error("thread_%lu: kp_kv_master_destroy() returned error=%d\n",
                tid, rc);
       return;
       kp_die("kp_kv_master_destroy() failed\n");
     }
     break;

   #ifdef CASSANDRA_INSTALLED
   case USE_CASSANDRA:
     node = (cassandra_node *) store;
     rc = cassandra_stop(node);
     if(rc != 0){
       kp_error("thread_%lu: cassandra_stop() returned error=%d\n",
                tid, rc);
       return;
     }
     break;
   #endif

   case USE_LEVELDB:
     db = (leveldb *) store;
     rc = leveldb_client_stop(db);
     if(rc != 0){
       kp_error("thread_%lu: leveldb_client_stop() returned error=%d\n",
                tid, rc);
       return;
     }
     break;
   default:
        kp_die("Request to use unknown KeyValueStore\n");
     break;
   }


 }

 void generic_worker_destroy(void *store){
   kp_kv_local *local;
   #ifdef CASSANDRA_INSTALLED
   int rc = 0;
   unsigned long tid = pthread_self();
   cassandra_node *node;
   #endif


   switch (WHICH_KEY_VALUE_STORE){
   case USE_KP_KV_STORE:
     local = (kp_kv_local *) store;
     kp_kv_local_destroy(&local);
     break;

   #ifdef CASSANDRA_INSTALLED
   case USE_CASSANDRA:
     node = (cassandra_node *) store;
     rc = cassandra_worker_stop(node);
     if(rc != 0){
       kp_error("thread_%lu: cassandra_stop() returned error=%d\n",
                tid, rc);
       return;
     }
     break;
   #endif

   case USE_LEVELDB:
     /* Since we use the master store for workers with leveldb, we
        only destroy in the master destroy  */
     return;
     break;
   default:
        kp_die("Request to use unknown KeyValueStore\n");
     break;
   }
 }

 void *generic_trans_start(void *store){
   leveldb *db;
   void *trans = NULL;
   #ifdef CASSANDRA_INSTALLED
   cassandra_node *node;
   #endif

   if(!store){
     kp_error("generic_trans_start recieved null argument\n");
     return NULL;
   }


   switch ( WHICH_KEY_VALUE_STORE ) {
   case USE_KP_KV_STORE:
     /* We don't have a formal transaction begin, it's implicit */
     break;
   case USE_CASSANDRA:
     /* Cassandra doesn't have anything like a transaction??? */
     break;
   case USE_LEVELDB:
     db = (leveldb *)store;
     trans = leveldb_client_start_trans(db);
     if(trans == NULL){
       kp_error("leveldb_client_start_trans() returned NULL\n");
     }
     break;
   default:
     kp_die("Request to use unknown KeyValueStore\n");
     break;
   }

   return trans;
 }

 /* Returns: 0 on success or error, 1 if there was a conflict. */
 int generic_trans_end(void *store, void *trans){
   int rc, retval;
   kp_kv_local *kv;
   leveldb *db;
   #ifdef CASSANDRA_INSTALLED
   cassandra_node *node;
   #endif

   if(!store){
     kp_error("generic_trans_end recieved null argument\n");
     return 0;
   }

   retval = 0;

   switch ( WHICH_KEY_VALUE_STORE ) {
   case USE_KP_KV_STORE:
     kv = (kp_kv_local*)store;
     rc = kp_local_commit(kv, NULL);
     if (rc == 1) {
       kp_debug2("got a conflict on commit, returning 1\n");
       retval = 1;
     } else if (rc == 2) {
       kp_warn("tried to commit an empty kvstore; this is unexpected...\n");
     } else if (rc != 0) {
       kp_error("kp_local_commit returned error=%d\n", rc);
       #ifdef KP_ASSERT
       kp_die("kp_local_commit returned error=%d\n", rc);
       #endif
     }
     break;
   case USE_CASSANDRA:
     /* Cassandra doesn't have anything like a transaction??? */
     break;
   case USE_LEVELDB:
     if(!trans){
       kp_error("generic_trans_end recieved null argument\n");
       return 0;
     }
     db = (leveldb*)store;
     rc = leveldb_client_stop_trans(db, trans);
     if(rc != 0){
       kp_error("leveldb_client_stop_trans returned error=%d\n", rc);
       #ifdef KP_ASSERT
       kp_die("kp_local_commit returned error=%d\n", rc);
       #endif
     }
     break;
   default:
     kp_die("Request to use unknown KeyValueStore\n");
     break;
   }

   return retval;
 }
 /* Returns: 0 if put succeeded, -1 if put failed. */
 int generic_put(void *store, void * trans, char *key, char *value, size_t size){
   int rc, retval;
   uint64_t rc64;
   kp_kv_local *kv;
   leveldb *db;

   #ifdef CASSANDRA_INSTALLED
   cassandra_node *node;
   #endif

   retval = 0;
   #ifdef KP_ASSERT
   if (!key) {
     kp_die("key is NULL!\n");
   }
   #endif

   switch ( WHICH_KEY_VALUE_STORE ) {
   case USE_KP_KV_STORE:
     kv = (kp_kv_local *)store;
     rc64 = kp_local_put(kv, key, value, size);
     if (rc64 == UINT64_MAX) {
       retval = -1;
       kp_error("kp_local_put(%s, %s, %u) failed\n", key, value, size);
   #ifdef KP_ASSERT
       kp_die("kp_local_put(%s, %s, %u) failed\n", key, value, size);
   #endif
     }
     break;

   case USE_LEVELDB:
     db = (leveldb *)store;
     //     if(!trans)
//       rc = leveldb_client_put(db, key, value);
//     else
       rc = leveldb_client_batch_put(db, trans, key, value);
     if (rc != 0) {
       retval = -1;
       kp_error("leveldb_client_put(%s, %s) failed\n", key, value);
   #ifdef KP_ASSERT
       kp_die("leveldb_client_put(%s, %s) failed\n", key, value);
   #endif
     }
     break;

   #ifdef CASSANDRA_INSTALLED
   case USE_CASSANDRA:
     node = (cassandra_node *)store;
     rc = cassandra_put(node, key, value);
     if (rc != 0) {
       retval = -1;
       kp_error("cassandra_put(%s, %s) failed\n", key, value);
   #ifdef KP_ASSERT
       kp_die("cassandra_put(%s, %s) failed\n", key, value);
   #endif
     }
     break;
   #endif

   default:
     kp_die("Request to use unknown KeyValueStore\n");
     break;
   }

   thpt_numputs++;
   return retval;
 }

 /* Returns: 0 if the get succeeded, -1 if the get failed. */
 int generic_get(void *store, char *key, char **value, size_t* size){
   int rc, retval;
   kp_kv_local *kv;
   leveldb *db;
   #ifdef CASSANDRA_INSTALLED
   cassandra_node *node;
   #endif

   retval = 0;

   switch ( WHICH_KEY_VALUE_STORE ) {
   case USE_KP_KV_STORE:
     kv = (kp_kv_local *)store;
     rc = kp_local_get(kv, key, (void **)value, size);
     if (rc != 0) {
       if (rc == 3) {  //tombstone, may be expected
         if(K_DEBUG) {
           printf("DEBUG: kp_local_get() got a tombstone for key=%s\n",
               key);
         }
       } else {  //anything else, not expected
         retval = -1;
         /* Because not all gets will succeed in multithreaded
            You can let me know, but don't fail out */
         kp_log(log_file, "kp_local_get(%s) failed (%d: %s)\n",
                key, rc, kp_retval_to_str(rc));
         /*        #ifdef KP_ASSERT
         kp_die("kp_local_get(%s) failed (%d: %s)\n",
             key, rc, kp_retval_to_str(rc));
             #endif */
       }
     }
     break;

   #ifdef CASSANDRA_INSTALLED
   case USE_CASSANDRA:
     node = (cassandra_node *)store;
     rc = cassandra_get(node, key, value);
     if (rc != 0) {
       retval = -1;
       if (rc == 1) {  //key not found
         kp_log(log_file, "cassandra_get(%s) returned key not found\n", key);
       } else {  //other error
         kp_log(log_file, "cassandra_get(%s) returned error: %d\n", key, rc);
       }
       /*#ifdef KP_ASSERT
       kp_die("cassandra_get(%s) failed, rc=%d\n", key, rc);
       #endif*/
     }
     break;
   #endif

   case USE_LEVELDB:
     db = (leveldb *)store;
     /* Last argument is "add_null_zero": should be set to false for
      * performance comparison. Setting it to true allows the gotten
      * values to be printed coherently, but adds an extra malloc +
      * strcpy overhead.
      */
     rc = leveldb_client_get(db, key,  value, false);
     if (rc != 0) {
       retval = -1;
       if (rc == 1) {  //key not found
         kp_log(log_file, "leveldb_client_get(%s) returned key not found\n", key);
       } else {  //other error
         kp_log(log_file, "leveldb_client_get(%s) returned error: %d\n", key, rc);
       }
       /*#ifdef KP_ASSERT
       kp_die("leveldb_client_get(%s) failed, rc=%d\n", key, rc);
       #endif */
     }
     break;

   default:
     kp_die("Request to use unknown KeyValueStore\n");
     break;
   }

   thpt_numgets++;
   return retval;
 }

 /* Returns: 0 if the delete succeeded, -1 if the delete failed. */
 int generic_delete(void *store, void *trans, char *key){
   int rc, retval;
   kp_kv_local *kv;
   leveldb *db;
   #ifdef CASSANDRA_INSTALLED
   cassandra_node *node;
   #endif

   retval = 0;

   switch ( WHICH_KEY_VALUE_STORE ) {
   case USE_KP_KV_STORE:
     kv = (kp_kv_local *)store;
     rc = kp_local_delete_key(kv, key);
     if (rc != 0) {
       retval = -1;
       if (rc == 1) {
         kp_error("kp_local_delete_key(%s) returned key not found\n", key);
       } else {
         kp_error("kp_local_delete_key(%s) returned error: %d\n", key, rc);
       }
       #ifdef KP_ASSERT
       kp_die("kp_local_delete_key(%s) returned error: %d\n", key, rc);
       #endif
     }
     break;

   #ifdef CASSANDRA_INSTALLED
   case USE_CASSANDRA:
     node = (cassandra_node *)store;
     rc = cassandra_delete(node, key);
     if (rc != 0) {
       retval = -1;
       if (rc == 1) {
         kp_error("cassandra_delete(%s) returned key not found\n", key);
       } else {
         kp_error("cassandra_delete(%s) returned error: %d\n", key, rc);
       }
       #ifdef KP_ASSERT
       kp_die("cassandra_delete(%s) returned error: %d\n", key, rc);
       #endif
     }
     break;
     #endif

   case USE_LEVELDB:
     db = (leveldb *)store;
     if(!trans)
       rc = leveldb_client_delete(db, key);
     else
       rc = leveldb_client_batch_delete(db, trans, key);
     if (rc != 0) {
       retval = -1;
       if (rc == 1) {
         kp_error("leveldb_client_delete(%s) returned key not found\n", key);
       } else {
         kp_error("leveldb_client_delete(%s) returned error: %d\n", key, rc);
       }
       #ifdef KP_ASSERT
       kp_die("leveldb_client_delete(%s) returned error: %d\n", key, rc);
       #endif
     }
     break;

   default:
     kp_die("Request to use unknown KeyValueStore\n");
     break;
   }

   thpt_numdels++;
   return retval;
 }

 void reset_thpt(void){
   thpt_numputs = 0;
   thpt_numgets = 0;
   thpt_numdels = 0;
 }


 void reset_counts(void){
   num_keys = 0;
   reset_thpt();
 }


 /* Uses a biased flip to determine if it's a put-new or put-old */
 int biased_update_coin(random_ints *ints){
   int r;
   if (!ints)
     kp_die("ints is NULL!\n");
   r = random_ints_next(ints);

   float rd = (float)r/(float)RAND_MAX;
   if(rd > update_probability)
       return 0; // put
   else
     return 1;   // update
 }

 /* Uses a biased flip to determine if it's a put or a get */
 int biased_put_get_coin(random_ints *ints){
   int r;
   if (!ints)
     kp_die("ints is NULL!\n");
   r = random_ints_next(ints);

   float rd = (float)r/(float)RAND_MAX;
   if(rd > put_probability)
       return 1; // gets
   else
     return 0;   // puts
 }

 /* Pick any random key from those we've added (for gets/deletes/updates) */
 int pick_random_key(int input_num_keys, random_ints *ints){
   int r;
   if (!ints)
     kp_die("ints is NULL!\n");
   r = random_ints_next(ints);

   float rd = (float)r/(float)RAND_MAX ;
   r = rd*(input_num_keys-1);
   if(K_DEBUG)
     printf("(%d/%d)\n", r, input_num_keys);
   return r;
 }


 /* Creates a random n-character null-terminated string for a key */
 int create_random_key(int size, char *key){
   int i, r,rc = 0;
   float rd; 
   char c;

   sprintf(key, "%c", 0 );
   for (i = 0; i < size; i++){
     r = random();
     rd = (float)r/(float)RAND_MAX;
     r = rd*26;
     c = r + 'a';
     key[i] = c;
   } 
   key[size] = '\0';
   if(K_DEBUG)
       printf("DEBUG: \t random string is: %s\n", key);

   if(key == NULL)
     printf("EWPS: issue with random string creation\n");
   return rc;
 }

 /* Creates a random string of n bytes for a value */
 int create_random_value(int size, void *value){
   int rc = 0;
   rc = create_random_key(size, (char*) value);
   return rc;

 }

 /* Allocates a struct for storing thread-local random integers.
  * srandom() must have been called before calling this function!!!
  *
  * Returns: 0 on success, -1 on error. On success, *ints is set
  * to point to the newly allocated random_ints struct. */
 int random_ints_create(random_ints **ints, unsigned int count)
 {
   unsigned int i;

   if (!ints || count == 0) {
     kp_error("invalid argument: ints=%p, count=%u\n", ints, count);
     return -1;
   }

   /* Need to allocate the struct itself, then allocate the array: */
   *ints = malloc(sizeof(random_ints));
   if (! *ints) {
     kp_error("malloc(*ints) failed\n");
     return -1;
   }
   (*ints)->array = malloc(sizeof(int) * count);
   if (! (*ints)->array) {
     kp_error("malloc((*ints)->array) failed\n");
     free(*ints);
     return -1;
   }

   /* Use random() instead of rand() - both are pseudo-random, but random()
    * is alleged to have better quality. */
   for (i = 0; i < count; i++) {
     ((*ints)->array)[i] = (int)random();
   }
   (*ints)->count = count;
   (*ints)->idx = 0;

   return 0;
 }

 /* Don't pass NULL to this function, or you will pay dearly!! */
 int random_ints_next(random_ints *ints)
 {
   int i = ints->array[ints->idx];
   ints->idx += 1;
   if (ints->idx == ints->count) {
     //    kp_warn("cycling around random_ints array!\n");
     ints->idx = 0;
   }

   //kp_print("returning random int=%d; count=%u, idx=%u\n",
   //   i, ints->count, ints->idx);
   return i;
 }

 /* Frees a random_ints struct. */
 void random_ints_destroy(random_ints *ints)
 {
   if (ints) {
     if (ints->array) {
       free(ints->array);
     }
     free(ints);
   }
 }

 int random_on_single_key(void *store, void *trans, int i, char* key,
     random_ints *ints){
   int key_num = -1;
   int retval, rc = 0;
   void *value = NULL;
   size_t size;

   /* Pick put or get */
   rc = biased_put_get_coin(ints);
   if(rc  == 0){    
     if(K_DEBUG){
       printf("DEBUG: Request is a put...\n");
       printf("PICKING TO DO A NORMAL UPDATE %d\n", key_num);
       printf("DEBUG: \tof an old key...\n");
       printf("DEBUG: \tkey is %s...\n", key);
     }

     /* pick a value */
     value = temp_value;
     //eval_debug("value from temp_values is %s\n", value);
     if(K_DEBUG)
       printf("DEBUG: \tvalue is %s...\n", (char*) value);

     /* Change the value: */
     rc = generic_put(store, trans, key, value, value_size+1);
     if (rc == 0)
       retval = 1;  //put-append
     else
       retval = -1;
   }
   else{
     /* If get...*/
     if(K_DEBUG){
       printf("DEBUG: Request is a get...\n");
       printf("PICKING TO DO A NORMAL GET %d\n", key_num);
       printf("DEBUG: \tkey is %s...\n", key);
     }

     /* Do the get: no version specification */
     if( key == NULL ){
       kp_die("got a NULL key for key_num %d; something is wrong\n",
           key_num);
       printf("NULL KEY VALUE IN UPDATE KEY %d\n", key_num);
       return -1;
     }
     if( store == NULL ){
       printf("NULL STORE VALUE IN UPDATE KEY %d\n", key_num);
       return -1;
     }
     rc = generic_get(store, key, (char **)&value, &size);
     if (rc == 0)
       retval = 2;  //get
     else
       retval = -1;

     free(value);
   }

   return retval;
 }

 /* Picks a request based on the global parameters listed above and
  * performs that request.
  * Returns: -1 if there was an error, 0 if the random request was a
  * put-insert, 1 if the random request was a put-append, or 2 if the
  * random request was a get.
  */
 int random_on_partitioned_keys(void *store, void *trans, int i, 
                                int offset, int *partition_count,
                                random_ints *ints){
   int key_num = -1;
   int retval, rc = 0;
   void *value = NULL;
   size_t size;
   char *key = NULL;

   /* Pick put or get */
   rc = biased_put_get_coin(ints);
   if((*partition_count < max_kv_size) && 
      ((*partition_count == 0) || (rc == 0))){
     if(K_DEBUG)
       printf("DEBUG: Request is a put...\n");

     rc = biased_update_coin(ints);

     /* pick update or append */
     if((*partition_count == 0) | (rc == 0)){
       if(K_DEBUG){
         printf("PICKING TO DO A NORMAL INSERT\n");
         printf("DEBUG: \tof a new key...\n");
       }

       /* Make a new key. Increment num_keys, which is the maximum index
        * into the temp_keys array that other operations can choose. The
        * copy-and-increment of num_keys would ideally be atomic, but it
        * two threads interleave here, the worst that can happen is that
        * both threads get the same key_num, and hence one of the put-
        * inserts will actually be a put-append, but that's not terrible
        * and will hopefully be rare. Assuming that the num_keys++ is
        * atomic (which may not be a safe assumption), then we should
        * never "lose" an update to num_keys here... and even if we do,
        * again it should just mean that the next time around we'll
        * perform a put-append rather than a put-insert. */
       key_num = *partition_count + offset;
       (*partition_count)++;
       key = temp_keys[key_num];

       if(K_DEBUG)
         printf("DEBUG: \tkey is %s...\n", key); 

       /* pick a value */
       value= temp_value;
       //eval_debug("value from temp_values is %s\n", value);
       if(K_DEBUG)
         printf("DEBUG: \tvalue is %s...\n", (char*) value);

       /* Set its value: */
       rc = generic_put(store, trans, key, value, value_size+1);
       if (rc == 0)
         retval = 0;  //put-insert
       else
         retval = -1;
     }
     else{
       /* Randomly choose a key from the range of keys that we've already
        * put to: */
       key_num = pick_random_key(*partition_count, ints) + offset;
       if(K_DEBUG){
         printf("PICKING TO DO A NORMAL UPDATE %d\n", key_num);
         printf("DEBUG: \tof an old key...\n");
       }

       /* key_num was already chosen to be a random index from 0 to the
        * number of keys that we've put so far (num_keys), so we can just
        * get the key pointer directly out of temp_keys, and it should
        * never be null. */
       key = temp_keys[key_num];
       if(K_DEBUG)
         printf("DEBUG: \tkey is %s...\n", key);

       /* pick a value */
       value = temp_value;
       //eval_debug("value from temp_values is %s\n", value);
       if(K_DEBUG)
         printf("DEBUG: \tvalue is %s...\n", (char*) value);

       /* Change the value: */
       rc = generic_put(store, trans, key, value, value_size+1);
       if (rc == 0)
         retval = 1;  //put-append
       else
         retval = -1;

     }
   }
   else{
     /* If get...*/
     if(K_DEBUG)
       printf("DEBUG: Request is a get...\n");

     /* Randomly choose a key from the range of keys that we've already
      * put to: */
     key_num = pick_random_key(*partition_count, ints) + offset;
     if(K_DEBUG)
       printf("PICKING TO DO A NORMAL GET %d\n", key_num);

     /* Get the key pointer directly out of temp_keys; it should never be
      * null. */
     key = temp_keys[key_num];
     if(K_DEBUG)
       printf("DEBUG: \tkey is %s...\n", key);

     /* Do the get: no version specification */

     if( key == NULL ){
       kp_die("got a NULL key for key_num %d; something is wrong\n",
           key_num);
       printf("NULL KEY VALUE IN UPDATE KEY %d\n", key_num);
       return -1;
     }
     if( store == NULL ){
       printf("NULL STORE VALUE IN UPDATE KEY %d\n", key_num);
       return -1;
     }
     rc = generic_get(store, key, (char **)&value, &size);
     if (rc == 0)
       retval = 2;  //get
     else
       retval = -1;

     free(value);
   }

   return retval;
 }

 int create_random_request(void *store, void *trans, int i, random_ints *ints){
   int key_num = -1;
   int retval, rc = 0;
   void *value = NULL;
   size_t size;
   char *key = NULL;

   /* Pick put or get */
   rc = biased_put_get_coin(ints);
   if((num_keys < max_kv_size) && ((num_keys == 0) || (rc == 0))){
     if(K_DEBUG)
       printf("DEBUG: Request is a put...\n");

     rc = biased_update_coin(ints);

     /* pick update or append */
     if((num_keys == 0) | (rc == 0)){
       if(K_DEBUG){
         printf("PICKING TO DO A NORMAL INSERT\n");
         printf("DEBUG: \tof a new key...\n");
       }

       /* Make a new key. Increment num_keys, which is the maximum index
        * into the temp_keys array that other operations can choose. The
        * copy-and-increment of num_keys would ideally be atomic, but it
        * two threads interleave here, the worst that can happen is that
        * both threads get the same key_num, and hence one of the put-
        * inserts will actually be a put-append, but that's not terrible
        * and will hopefully be rare. Assuming that the num_keys++ is
        * atomic (which may not be a safe assumption), then we should
        * never "lose" an update to num_keys here... and even if we do,
        * again it should just mean that the next time around we'll
        * perform a put-append rather than a put-insert. */
       key_num = num_keys;
       num_keys++;
       key = temp_keys[key_num];
       eval_debug("WORKER: picked new key=%d (of %d so far) for put-insert; "
           "merge_every=%d\n", key_num, num_keys, merge_every);

       if(K_DEBUG)
         printf("DEBUG: \tkey is %s...\n", key); 

       /* pick a value */
       value = temp_value;
       //eval_debug("value from temp_values is %s\n", value);
       if(K_DEBUG)
         printf("DEBUG: \tvalue is %s...\n", (char*) value);

       /* Set its value: */
       rc = generic_put(store, trans, key, value, value_size+1);
       if (rc == 0)
         retval = 0;  //put-insert
       else
         retval = -1;
     }
     else{
       /* Randomly choose a key from the range of keys that we've already
        * put to: */
       key_num = pick_random_key(num_keys, ints);
       if(K_DEBUG){
         printf("PICKING TO DO A NORMAL UPDATE %d\n", key_num);
         printf("DEBUG: \tof an old key...\n");
       }
       eval_debug("WORKER: picked random key=%d (of %d so far) for put-append; "
           "merge_every=%d\n", key_num, num_keys, merge_every);

       /* key_num was already chosen to be a random index from 0 to the
        * number of keys that we've put so far (num_keys), so we can just
        * get the key pointer directly out of temp_keys, and it should
        * never be null. */
       key = temp_keys[key_num];
       if(K_DEBUG)
         printf("DEBUG: \tkey is %s...\n", key);

       /* pick a value */
       value = temp_value;
       //eval_debug("value from temp_values is %s\n", value);
       if(K_DEBUG)
         printf("DEBUG: \tvalue is %s...\n", (char*) value);

       /* Change the value: */
       rc = generic_put(store, trans, key, value, value_size+1);
       if (rc == 0)
         retval = 1;  //put-append
       else
         retval = -1;

     }
  }
  else{
    /* If get...*/
    if(K_DEBUG)
      printf("DEBUG: Request is a get...\n");

    /* Randomly choose a key from the range of keys that we've already
     * put to: */
    key_num = pick_random_key(iterations, ints);
    if(K_DEBUG)
      printf("PICKING TO DO A NORMAL GET %d\n", key_num);
    eval_debug("WORKER: picked random key=%d (of %d so far) for get; "
        "merge_every=%d\n", key_num, num_keys, merge_every);

    /* Get the key pointer directly out of temp_keys; it should never be
     * null. */
    key = temp_keys[key_num];
    if(K_DEBUG)
      printf("DEBUG: \tkey is %s...\n", key);

    /* Do the get: no version specification */

    if( key == NULL ){
      kp_die("got a NULL key for key_num %d; something is wrong\n",
          key_num);
      printf("NULL KEY VALUE IN UPDATE KEY %d\n", key_num);
      return -1;
    }
    if( store == NULL ){
      printf("NULL STORE VALUE IN UPDATE KEY %d\n", key_num);
      return -1;
    }
    rc = generic_get(store, key, (char **)&value, &size);
    if (rc == 0)
      retval = 2;  //get
    else
      retval = -1;

    free(value);
  }

  return retval;
}


/* Tests the latency of an average workload composed of all the
 * commands currently in our arsenal. Reports results as timing.
 * Only collects info of successful calls. 0 success, -1 failure
 * TODO: workload has weird spikes. check out return values, maybe ?
 */
int workload_evaluation(int num_iterations, void *store, random_ints *ints){
  int i, rc = 0;
  kp_timers timers;
  int error_count = 0;
  void * trans = NULL;
  /* Start random workload, get cost of average call */
  struct timeval *start = malloc(sizeof(struct timeval));
  if (!start)
    kp_die("malloc(start) failed\n");
  struct timeval *end = malloc(sizeof(struct timeval));
  if (!end)
    kp_die("malloc(end) failed\n");

  printf("***NOW DOING LATENCY TEST ON RANDOM WORKLOAD***\n");

  /* create a random workload */
  bool trans_merged=true;
  int num_puts = 0;
  int ret = 0, conflicts = 0, commits = 0;

  reset_thpt();
  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(1);

  trans = generic_trans_start(store);
  for (i=0; i < num_iterations; i++){
    rc = create_random_request(store, trans,  i, ints);
    if (rc == -1) {
      error_count++;
      kp_log(log_file,
             "create_random_request() returned error, looping again\n");
      continue;
    } else if (rc == 0 || rc == 1) {
      num_puts++;
      trans_merged = false;
    }

    /* Merge every once in a while: only track puts, ignore the gets! */
    if ((num_puts % merge_every) == 0 
        && !trans_merged) {  //don't merge on first put
      if(K_DEBUG)
        printf("merging at %u puts (%d loops)\n", num_puts, i);
      ret = generic_trans_end(store, trans);
      if (ret)
        conflicts++;
      commits++;
      eval_debug("LATENCY: (workload) just committed (%d)\n", commits);
      trans_merged=true;
      trans = generic_trans_start(store);
    }
  }
  if(!trans_merged) {
    ret = generic_trans_end(store, trans);
    if (ret)
      conflicts++;
    commits++;
    eval_debug("LATENCY: (workload) just committed (%d)\n", commits);
  }
  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);
  int saved_puts = thpt_numputs;
  int saved_gets = thpt_numgets;
  int saved_dels = thpt_numdels;
  int total_ops = saved_puts+saved_gets+saved_dels;

  printf("create_random_request() returned %d errors during loop\n", 
           error_count);
  /* Get timing information */
  int usec,  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  int time_usec = 1000000*sec+usec;
  float cost_workload = (float)time_usec/(float)num_iterations;

  /* Get throughput information */
  


  /* Report reuslts */
  printf("For random_workload: \n\t time elapsed %d:%d (sec:usec)"
	 "= %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n", 
	 num_iterations, cost_workload);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n",
         1000000*(float)total_ops/(float)time_usec);
  printf("\t Conflicts: %d (of %d commits)\n\n", conflicts, commits);
  print_timing("RANDOM-WORKLOAD", &timers, num_iterations);

  free(start);
  free(end);
  return rc;
}

/* Tests the latency of each operation currently in our arsenal
 * put-insert, put-append, get, delete, garbage collect.
 * Reports the latency as timing information. 
 * Only collects info on successful calls. 0 success, -1 failure
 */
int latency_evaluation(int num_iterations, void* store){
  int actual_count, key_num, i, j, rc = 0;
  void *value = NULL;
  size_t size;
  char *key = NULL;
  void *trans;
  int ret = 0, conflicts = 0, commits = 0;;

  float cost_put_insert = 0;
  float cost_put_append = 0;
  float cost_get_current = 0;
  struct timeval *start = malloc(sizeof(struct timeval));
  if (!start)
    kp_die("malloc(start) failed\n");
  struct timeval *end = malloc(sizeof(struct timeval));
  if (!end)
    kp_die("malloc(end) failed\n");
  kp_timers timers;

  printf("***NOW DOING LATENCY TEST ON INDIVIDUAL FUNCTIONS ***\n");

  int saved_puts = thpt_numputs;
  int saved_gets = thpt_numgets;
  int saved_dels = thpt_numdels;
  int total_ops = saved_puts + saved_gets + saved_dels;
  
  /* Figure out the timing */
  int usec,  sec ;
  int time_usec;

#if 0
  /* Get the average cost of put-insert */
  /*=================================================================*/
  /* Measure N iterations */
 rc = 0;
  actual_count = num_iterations;

  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(2);

  reset_thpt();
  trans = generic_trans_start(store);
  for(i = 0; i < num_iterations; i++){
    /* For put-insert, we "make" a new key by incrementing num_keys.
     * See more detailed comment in create_random_request() above
     * (there are minor concurrency issues here). */
    key_num = num_keys;
    num_keys++;
    key = temp_keys[key_num];
    eval_debug("LATENCY: picked new key=%d (of %d so far) for put-insert; "
        "merge_every=%d\n", key_num, num_keys, merge_every);

    if(K_DEBUG)
      printf("DEBUG: \tkey is %s...\n", key); 
    
    /* pick a value */
    value = temp_value;
    //eval_debug("value from temp_values is %s\n", value);
    if(K_DEBUG)
      printf("DEBUG: \tvalue is %s...\n", (char*) value);
    
    /* Set its value */
    rc = generic_put(store, trans, key, value, value_size+1);
    //    if (rc != 0) {
      //      actual_count--;  //error messages printed inside generic_put()
    //    }
    /* Merge every once in a while: */
    if ((i % merge_every) == 0 && 
        (i > 0 || merge_every ==1)) {  //don't merge on first loop
      if(K_DEBUG)
        printf("merging at %d\n", i);
      ret = generic_trans_end(store, trans);
      if (ret)
        conflicts++;
      commits++;
      eval_debug("LATENCY: just committed (%d)\n", commits);
      trans = generic_trans_start(store);
    }
  }
  if ((i%merge_every) != 0) {
    ret = generic_trans_end(store, trans);
    if (ret)
      conflicts++;
    commits++;
    eval_debug("LATENCY: just committed (%d)\n", commits);
  }

  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);
  int saved_puts = thpt_numputs;
  int saved_gets = thpt_numgets;
  int saved_dels = thpt_numdels;
  int total_ops = saved_puts + saved_gets + saved_dels;
  
  /* Figure out the timing */
  int usec,  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  int time_usec = 1000000*sec+usec;
  cost_put_insert = (float)time_usec/(float)actual_count;

  /* Print results */
  printf("For put-insert: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d (merge every %d -> %d merges)\n"
      "\t average cost %f usecs\n", 
      actual_count, merge_every, actual_count/merge_every, cost_put_insert);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n",
         1000000*(float)total_ops/(float)time_usec);
  printf("\t Conflicts: %d (of %d commits)\n\n", conflicts, commits);
  print_timing("PUT-INSERT", &timers, actual_count);

  /* Get the average cost of put-append */
  /*=================================================================*/
  /* Measure N iterations */
  conflicts = 0;
  commits = 0;
  actual_count = MULT_FACTOR * num_iterations;

  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(3);

  reset_thpt();
  trans  = generic_trans_start(store);
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    /* Don't change this loop setup [i] without seeing comment below!! */
    for(i = 0; i < num_iterations; i++){

      /* For put-append, we randomly choose a key from the range of keys
       * that we've already put to. In this function, we already put to
       * num_iterations keys (in the put-insert section above), so right
       * here, we just use our loop index to select the key. This ensures
       * that our put-appends are uniformly distributed over the key-space,
       * which is hopefully what we want. We can just get the key pointer
       * directly out of temp_keys, so it should never be null. */
      key = temp_keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
      eval_debug("LATENCY: picked sequential key=%d (of %d so far) for "
          "put-append; merge_every=%d\n", i, num_keys, merge_every);

      /* pick a value */
      value = temp_value;
      //eval_debug("value from temp_values is %s\n", value);
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* Set its value */
      rc = generic_put(store, trans, key, value, value_size+1);
      //      if (rc != 0) {
      //        actual_count--;  //error messages printed inside generic_put()
      //      }

      /* Merge every once in a while: */
      if ((i % merge_every) == 0 && 
          (i >0 || merge_every == 1)) {  //don't merge on first loop
        if(K_DEBUG)
          printf("merging at %d\n", i);
        ret = generic_trans_end(store, trans);
        if (ret)
          conflicts++;
        commits++;
        trans = generic_trans_start(store);
      }
    }
  }
  if((i% merge_every) != 0) {
    ret = generic_trans_end(store, trans);
    if (ret)
      conflicts++;
    commits++;
  }

  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);

  saved_puts = thpt_numputs;
  saved_gets = thpt_numgets;
  saved_dels = thpt_numdels;
  total_ops = saved_puts + saved_gets + saved_dels;

  /* Figure out the timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost_put_append = (float)time_usec/(float)(actual_count);

  /* Print results */
  printf("For put-append: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d (merge every %d -> %d merges)\n"
      "\t average cost %f usecs\n",
      actual_count, merge_every, actual_count/merge_every, cost_put_append);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n",
         1000000*(float)total_ops/(float)time_usec);
  printf("\t Conflicts: %d (of %d commits)\n\n", conflicts, commits);
  print_timing("PUT-APPEND", &timers, actual_count);

#endif  /* Get the average cost of get-current */
  /*=================================================================*/
  /* no need to create values, or keys. Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;

  gettimeofday(start, NULL);
  start_timing(&timers);

  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    /* Don't change this loop setup [i] without seeing comment below!! */
    for(i = 0; i < num_iterations; i++){

      /* For gets, we randomly choose a key from the range of keys
       * that we've already put to, using index i. This is the same as
       * we did for put-appends (see more comments above). */
      key = temp_keys[i];
      rc = generic_get(store, key, (char **)&dest_values[i], (size_t*)&size);

      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* DON'T merge while performing gets, just loop again. */
    }
  }

  stop_timing(&timers);
  gettimeofday(end, NULL);
  saved_puts = thpt_numputs;
  saved_gets = thpt_numgets;
  saved_dels = thpt_numdels;
  total_ops = saved_puts + saved_gets + saved_dels;

  /* Figure out the timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost_get_current = (float)time_usec/(float)(actual_count);
  
  /* Print results */
  printf("For get-current: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n", 
	 actual_count, cost_get_current);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n\n",
         1000000*(float)total_ops/(float)time_usec);
  print_timing("GET-CURRENT", &timers, actual_count);

  /* We're not timing right now, so should be ok to do this freeing here. */
  for( i = 0; i < num_iterations; i++){
    free(dest_values[i]);
  }

  /*=================================================================*/

  /* Free the kv store, reset counts, and free all those values */
  free(start);
  free(end);

  return rc;
}

int split_latency_evaluation(int num_iterations, void* store){
  int actual_count, key_num, i, j, rc = 0;
  void *value = NULL;
  size_t size;
  char *key = NULL;
  void *trans;
  int ret = 0, conflicts = 0, commits = 0;

  float cost_put_insert = 0;
  float cost_put_append = 0;
  float cost_get_current = 0;
  struct timeval *start = malloc(sizeof(struct timeval));
  if (!start)
    kp_die("malloc(start) failed\n");
  struct timeval *end = malloc(sizeof(struct timeval));
  if (!end)
    kp_die("malloc(end) failed\n");
  kp_timers timers;

  printf("***NOW DOING LATENCY TEST ON INDIVIDUAL FUNCTIONS ***\n");


  #ifdef CASSANDRA_INSTALLED
  /* For Cassandra, get the cost of a no-op: */
  /*=================================================================*/
  if (WHICH_KEY_VALUE_STORE == USE_CASSANDRA) {
    actual_count = num_iterations;
    gettimeofday(start, NULL);
    start_timing(&timers);
    INSTRUMENT_ON();
    CLEAR_COUNTS_SET_ID(5);

    for(i = 0; i < num_iterations; i++){
      rc = cassandra_noop1((cassandra_node *)store);
      //rc = cassandra_noop2((cassandra_node *)store);
      //rc = cassandra_noop3((cassandra_node *)store);
      //rc = cassandra_noop4((cassandra_node *)store);
      //rc = cassandra_noop5((cassandra_node *)store);
      if (rc != 0) {
        printf( "DEBUG: Error returned from cassandra_noop %d\n", rc);
        actual_count --;
      }
    }

    INSTRUMENT_OFF();
    stop_timing(&timers);
    gettimeofday(end, NULL);

    int usec,  sec = end->tv_sec - start->tv_sec;
    usec = end->tv_usec - start->tv_usec;
    if(end->tv_usec < start->tv_usec){
      sec --;
      usec += 1000000;
    }
    int time_usec = 1000000*sec+usec;
    cost_put_insert = (float)time_usec/(float)actual_count;

    printf("For cassandra no-op:\n\ttime elapsed %d:%d(sec:usec) = %d usecs\n",
  	 sec, usec, time_usec);
    printf("\t iterations %d \n\t average cost %f usecs\n\n", 
  	 actual_count, cost_put_insert);
    print_timing("CASSANDRA NO-OP", &timers, actual_count);
  }
  #endif


  /* Get the average cost of put-insert TO LOCAL STORE (no merge) */
  /*=================================================================*/
  /* Measure N iterations */
  rc = 0;
  actual_count = num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(6);
  
  reset_thpt();
  trans  = generic_trans_start(store);
  for(i = 0; i < num_iterations; i++){
    /* For put-insert, we "make" a new key by incrementing num_keys.
     * See more detailed comment in create_random_request() above
     * (there are minor concurrency issues here). */
    key_num = num_keys;
    num_keys++;
    key = temp_keys[key_num];

    if(K_DEBUG)
      printf("DEBUG: \tkey is %s...\n", key); 
    
    /* pick a value */
    value = temp_value;
    //eval_debug("value from temp_values is %s\n", value);
    if(K_DEBUG)
      printf("DEBUG: \tvalue is %s...\n", (char*) value);
    
    /* Set its value */
    rc = generic_put(store, trans, key, value, value_size+1);
    if (rc != 0) {
      actual_count--;  //error messages printed inside generic_put()
    }
  }

  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);
  int saved_puts = thpt_numputs;
  int saved_gets = thpt_numgets;
  int saved_dels = thpt_numdels;
  int total_ops = saved_puts + saved_gets + saved_dels;
  
  /* Figure out the timing */
  int usec,  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  int time_usec = 1000000*sec+usec;
  cost_put_insert = (float)time_usec/(float)actual_count;

  /* Print results */
  printf("For put-insert: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d (merge every %d -> %d merges)\n"
      "\t average cost %f usecs\n", 
      actual_count, merge_every, actual_count/merge_every, cost_put_insert);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n\n",
         1000000*(float)total_ops/(float)time_usec);
  print_timing("PUT-INSERT", &timers, actual_count);

  /* Get the average cost of put-append to LOCAL STORE (no merge)*/
  /*=================================================================*/
  /* Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(7);

  reset_thpt();
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    /* Don't change this loop setup [i] without seeing comment below!! */
    for(i = 0; i < num_iterations; i++){

      /* For put-append, we randomly choose a key from the range of keys
       * that we've already put to. In this function, we already put to
       * num_iterations keys (in the put-insert section above), so right
       * here, we just use our loop index to select the key. This ensures
       * that our put-appends are uniformly distributed over the key-space,
       * which is hopefully what we want. We can just get the key pointer
       * directly out of temp_keys, so it should never be null. */
      key = temp_keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);

      /* pick a value */
      value = temp_value;
      //eval_debug("value from temp_values is %s\n", value);
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* Set its value */
      rc = generic_put(store, trans, key, value, value_size+1);
      if (rc != 0) {
        actual_count--;  //error messages printed inside generic_put()
      }
    }
  }

  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);
  saved_puts = thpt_numputs;
  saved_gets = thpt_numgets;
  saved_dels = thpt_numdels;
  total_ops = saved_puts + saved_gets + saved_dels;

  /* Figure out the timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost_put_append = (float)time_usec/(float)(actual_count);

  /* Print results */
  printf("For put-append: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d (merge every %d -> %d merges)\n"
      "\t average cost %f usecs\n",
      actual_count, merge_every, actual_count/merge_every, cost_put_append);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n\n",
         1000000*(float)total_ops/(float)time_usec);
  print_timing("PUT-APPEND", &timers, actual_count);


  /* Get the average cost of get-current to local store*/
  /*=================================================================*/
  /* no need to create values, or keys. Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(8);

  reset_thpt();
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    /* Don't change this loop setup [i] without seeing comment below!! */
    for(i = 0; i < num_iterations; i++){

      /* For gets, we randomly choose a key from the range of keys
       * that we've already put to, using index i. This is the same as
       * we did for put-appends (see more comments above). */
      key = temp_keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
 
      /* Get its value */
      /* HACK MASSIVE MEMORY LEAK */
      //      rc = generic_get(store, key, (char **)&dest_value, (size_t*)&size);
      rc = generic_get(store, key, (char **)&dest_values[i], (size_t*)&size);

      if (rc != 0) {
        actual_count--;  //error messages printed inside generic_get()
      }

      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* DON'T merge while performing gets, just loop again. */
    }
  }
  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);
  saved_puts = thpt_numputs;
  saved_gets = thpt_numgets;
  saved_dels = thpt_numdels;
  total_ops = saved_puts + saved_gets + saved_dels;

  /* Figure out the timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost_get_current = (float)time_usec/(float)(actual_count);
  
  /* Print results */
  printf("For get-current: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n", 
	 actual_count, cost_get_current);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n\n",
         1000000*(float)total_ops/(float)time_usec);
  print_timing("GET-CURRENT", &timers, actual_count);

  /* We're not timing right now, so should be ok to do this freeing here. */
  for( i = 0; i < num_iterations; i++){
    free(dest_values[i]);
  }

  /*=================================================================*/
  /* Merge them all in and now operate solely on a cold store*/
  ret = generic_trans_end(store, trans);
  if (ret)
    kp_die("got a conflict from generic_trans_end()!?!?\n");
  /*=================================================================*/



  /* Get the average cost of put-insert TO MASTER STORE  */
  /*=================================================================*/
  /* Measure N iterations */
  conflicts = 0;
  commits = 0;
  rc = 0;
  actual_count = num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(9);

  reset_thpt();
  trans  = generic_trans_start(store);
  for(i = 0; i < num_iterations; i++){
    /* For put-insert, we "make" a new key by incrementing num_keys.
     * See more detailed comment in create_random_request() above
     * (there are minor concurrency issues here). */
    key_num = num_keys;
    num_keys++;
    key = temp_keys[key_num];

    if(K_DEBUG)
      printf("DEBUG: \tkey is %s...\n", key); 
    
    /* pick a value */
    value = temp_value;
    //eval_debug("value from temp_values is %s\n", value);
    if(K_DEBUG)
      printf("DEBUG: \tvalue is %s...\n", (char*) value);
    
    /* Set its value */
    rc = generic_put(store, trans, key, value, value_size+1);
    if (rc != 0) {
      actual_count--;  //error messages printed inside generic_put()
    }
    /* do the merge */
    ret = generic_trans_end(store, trans);
    if (ret)
      conflicts++;
    commits++;
    if(i != num_iterations -1)
      trans  = generic_trans_start(store);
  }

  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);
  saved_puts = thpt_numputs;
  saved_gets = thpt_numgets;
  saved_dels = thpt_numdels;
  total_ops = saved_puts + saved_gets + saved_dels;
  
  /* Figure out the timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost_put_insert = (float)time_usec/(float)actual_count;

  /* Print results */
  printf("For put-insert: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d (merge every %d -> %d merges)\n"
      "\t average cost %f usecs\n", 
      actual_count, merge_every, actual_count/merge_every, cost_put_insert);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n",
         1000000*(float)total_ops/(float)time_usec);
  printf("\t Conflicts: %d (of %d commits)\n\n", conflicts, commits);
  print_timing("PUT-INSERT", &timers, actual_count);

  /* Get the average cost of put-append to MASTER STORE */
  /*=================================================================*/
  /* Measure N iterations */
  conflicts = 0;
  commits = 0;
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(10);
  reset_thpt();
  trans = generic_trans_start(store);
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    /* Don't change this loop setup [i] without seeing comment below!! */
    for(i = 0; i < num_iterations; i++){

      /* For put-append, we randomly choose a key from the range of keys
       * that we've already put to. In this function, we already put to
       * num_iterations keys (in the put-insert section above), so right
       * here, we just use our loop index to select the key. This ensures
       * that our put-appends are uniformly distributed over the key-space,
       * which is hopefully what we want. We can just get the key pointer
       * directly out of temp_keys, so it should never be null. */
      key = temp_keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);

      /* pick a value */
      value = temp_value;
      //eval_debug("value from temp_values is %s\n", value);
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* Set its value */
      rc = generic_put(store, trans, key, value, value_size+1);
      if (rc != 0) {
        actual_count--;  //error messages printed inside generic_put()
      }

      /* do the merge */
      ret = generic_trans_end(store, trans);
      if (ret)
        conflicts++;
      commits++;
      if ((j != MULT_FACTOR-1) && (i != num_iterations -1))
        trans  = generic_trans_start(store);
    }
  }

  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);
  saved_puts = thpt_numputs;
  saved_gets = thpt_numgets;
  saved_dels = thpt_numdels;
  total_ops = saved_puts + saved_gets + saved_dels;

  /* Figure out the timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost_put_append = (float)time_usec/(float)(actual_count);

  /* Print results */
  printf("For put-append: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d (merge every %d -> %d merges)\n"
      "\t average cost %f usecs\n",
      actual_count, merge_every, actual_count/merge_every, cost_put_append);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n",
         1000000*(float)total_ops/(float)time_usec);
  printf("\t Conflicts: %d (of %d commits)\n\n", conflicts, commits);
  print_timing("PUT-APPEND", &timers, actual_count);


  /* Get the average cost of get-current to local store*/
  /*=================================================================*/
  /* no need to create values, or keys. Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(11);

  reset_thpt();
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    /* Don't change this loop setup [i] without seeing comment below!! */
    for(i = 0; i < num_iterations; i++){

      /* For gets, we randomly choose a key from the range of keys
       * that we've already put to, using index i. This is the same as
       * we did for put-appends (see more comments above). */
      key = temp_keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
 
      /* Get its value */
      /* HACK MASSIVE MEMORY LEAK */
      //      rc = generic_get(store, key, (char **)&dest_value, (size_t*)&size);

      rc = generic_get(store, key, (char **)&dest_values[i], (size_t*)&size);
      if (rc != 0) {
        actual_count--;  //error messages printed inside generic_get()
      }

      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* DON'T merge while performing gets, just loop again. */
    }
  }

  INSTRUMENT_OFF();
  stop_timing(&timers);
  gettimeofday(end, NULL);
  saved_puts = thpt_numputs;
  saved_gets = thpt_numgets;
  saved_dels = thpt_numdels;
  total_ops = saved_puts + saved_gets + saved_dels;

  /* Figure out the timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost_get_current = (float)time_usec/(float)(actual_count);
  
  /* Print results */
  printf("For get-current: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n", 
	 actual_count, cost_get_current);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n\n",
         1000000*(float)total_ops/(float)time_usec);
  print_timing("GET-CURRENT", &timers, actual_count);

  /* We're not timing right now, so should be ok to do this freeing here. */
  for( i = 0; i < num_iterations; i++){
    free(dest_values[i]);
  }


  /* Free the kv store, reset counts, and free all those values */
  free(start);
  free(end);

  return rc;
}



/* Calls the key-value store internal measurement function
 * to evaluate the size and proportions of the store
 * Works up some random workload first. 
 */
int local_keyvalue_size(int num_iterations, kp_kv_local *kv) {
  if( WHICH_KEY_VALUE_STORE != USE_KP_KV_STORE){
    printf("print stats not implemented for this typeof kv store\n");
    return 0;
  }

  int rc = 0;

  printf("***BEGINNING MEMORY FOOTPRINT EVALUATION***\n");  

  /* Create file name*/
  char* buf = malloc(32);
  if (!buf)
    kp_die("malloc(buf) failed\n");
  snprintf(buf, 32, "eval/memory_stats_%d_%d.out", key_size, value_size);

  /* Create stats report in file */
  FILE *fp = fopen(buf, "w");
  kp_local_print_stats(kv, fp, false);


  /* Close files, destroy kv, clean up the rest... */
  fclose(fp); 
  free(buf); 
  printf("***FINISHING MEMORY FOOTPRINT EVALUATION***\n");  
  return rc;
}



/* For the thread-test evaluations, keep each not-main thread
 * busy with a random workload constantly, to maximize numbers
 */
void *worker_thread_entrypoint(void *arg){
  int i, rc;
  int num_puts;
  bool trans_merged = true;
  char *ret_string;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  void *master;
  void *worker;
  void *trans = NULL;
  int error_count = 0;
  int ret = 0, conflicts = 0, commits = 0;
  random_ints *ints;

  /* setup thread */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;
  worker = generic_worker_create(master);
  num_puts = 0;
  ints = thread_args->ints;

  /* Do work */

  trans = generic_trans_start(worker);
  for (i=0; i < MT_EXTEND*iterations; i++){ //Extend long enough to get mt during lat
      rc = create_random_request(worker, trans, i, ints);
      if (rc == -1) {
        error_count++;
        kp_log(log_file,
               "create_random_request() returned error, looping again\n");
        continue;
      } else if (rc == 0 || rc == 1) {
        num_puts++;
        trans_merged = false;
      }
      
      //      /* Merge every once in a while: */
      //      if ((num_puts % merge_every) == 0 
      //          && !trans_merged) {  //don't merge on first put
      //        if(K_DEBUG)
      //          printf("merging at %u puts (%d loops)\n", num_puts, i);
      ret = generic_trans_end(worker, trans);
      if (ret)
        conflicts++;
      commits++;
      eval_debug("WORKER: just committed (%d)\n", commits);
      trans_merged = true;
      trans = generic_trans_start(worker);
      //      }
    }
  /*  if(!trans_merged) {
    ret = generic_trans_end(worker, trans);
    if (ret)
      conflicts++;
    commits++;
    eval_debug("WORKER: just committed (%d)\n", commits);
  }
  */

  /* Destroy (or fake-destroy) a worker */
  generic_worker_destroy(worker);
  printf("\t Conflicts in worker thread: %d (of %d commits)\n",
      conflicts, commits);
    //TODO: return this value to main thread, have it total the conflicts
    //  from all workers.

  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
  if (!ret_string)
    kp_die("malloc(ret_string) failed\n");
  snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
  if(K_DEBUG)
    printf("thread %lu returning string=%s\n", tid, ret_string);
  return (void *) ret_string;
}


void *measurement_thread_entrypoint(void *arg){
  char *ret_string;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  void *master;
  void *worker;
  random_ints *ints;

  /* setup thread */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;
  worker = generic_worker_create(master);
  ints = thread_args->ints;

  /* Do work */
  eval_debug("LATENCY: starting latency_evaluation()\n");
  latency_evaluation(iterations, worker);
  eval_debug("LATENCY: finished latency_evaluation(), starting "
      "workload_evaluation()\n");
  workload_evaluation(iterations, worker, ints);
  eval_debug("LATENCY: finished workload_evaluation()\n");


  /* Destroy (or fake-destroy) a worker */
  generic_worker_destroy(worker);
    //TODO: return this value to main thread, have it total the conflicts
    //  from all workers.

  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
  if (!ret_string)
    kp_die("malloc(ret_string) failed\n");
  snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
  if(K_DEBUG)
    printf("thread %lu returning string=%s\n", tid, ret_string);
  return (void *) ret_string;
}



/* Progressively increases the number of threads such that
 * we can find the bottle neck. After each increase in threads
 * measures the latency of each request type, and the average
 * latency of a random workload. Individual functions print results.
 */
void ramp_up_threads(int num_threads){
  int rc, i = 0;
  void *ret_thread;
  int cpu;
  pthread_attr_t attr;
  pthread_t threads[MAX_THREADS];
  benchmark_thread_args thread_args[MAX_THREADS];  

  /* Check args */
  if(num_threads <= 0){
    kp_die("invalid num_threads=%d\n", num_threads);
  }
  kp_debug("pid=%d, num_threads=%d\n", getpid(), num_threads);


  /* Create the master and one local worker*/
  void * master = generic_store_create();
  void * worker = generic_worker_create(master);

  /* Pre-allocate random numbers for each thread, _before_ starting any
   * of them: */
  for (i = 0; i < num_threads; i++) {
    rc = random_ints_create(&(thread_args[i].ints), random_int_count);
    if (rc != 0) {
      kp_error("random_ints_create() failed\n");
      return;
    }
  }
  
  void *trans = generic_trans_start(worker);
  for (i=0; i < iterations; i++){
      rc = generic_put(worker, trans, temp_keys[i], temp_value, value_size+1);
      num_keys++;
  }
  generic_trans_end(worker, trans);

  /* Create a single thread */
  for (i=0; i < num_threads; i++){
    /* Create attributes */
    rc = pthread_attr_init(&attr);
    if(rc != 0)
      kp_die("pthread_attr_init() returned error=%d\n", rc);
    
    /* Set CPU and affinity */
    cpu = CPU_OFFSET + (i % NUM_CPUS);
    CPU_ZERO(&(thread_args[i]).cpu_set);
    CPU_SET(cpu, &(thread_args[i]).cpu_set);
    rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t),
                                     &((thread_args[i]).cpu_set));
    if(rc != 0)
      kp_die("pthread_attr_setaffinity_np() returned error=%d\n", rc);
#ifdef PRINT_CPUS
    kp_print("pinned new thread to CPU=0x%X\n", cpu);
#endif
    
    /* Set the master and spawn */
    thread_args[i].master = master;
    if(K_DEBUG){
      printf("Creating threads to complete workload MT tasks\n");
    }
    
    if(i != num_threads -1){
      rc = pthread_create(&threads[i], &attr, &worker_thread_entrypoint, 
                          (void *)(&(thread_args[i])));
    }
    else{
      rc = pthread_create(&threads[i], &attr, &measurement_thread_entrypoint,
                          (void *)(&(thread_args[i])));
    }
    if(rc != 0) {
      if (rc == 22) {
        kp_error("pthread_create() returned error=%d: check that NUM_CPUS "
                 "(%d) is not greater than number of physical cores!\n", rc, NUM_CPUS);
      }
      kp_die("pthread_create() returned error=%d\n", rc);
    }
    
    
    /* Destroy the attributes */
    rc = pthread_attr_destroy(&attr);
    if(rc != 0)
      kp_die("pthread_attr_destroy() returned error=%d\n", rc);
  }
  /*=============== MASTER ACTIONS ==================== */

  /* wait on the measurement thread */
  rc = pthread_join(threads[num_threads-1], &ret_thread);
  if(rc != 0)
    kp_die("pthread_join() returned error=%d\n", rc);
  if(ret_thread)
    free(ret_thread);

  /* Get that worker back! */
  for( i = 0; i < num_threads-1; i++){
    rc = pthread_join(threads[i], &ret_thread);
    if(rc != 0)
      kp_die("pthread_join() returned error=%d\n", rc);
    if(ret_thread)
      free(ret_thread);
  }
  printf("***END OF RAMP-UP-THREADS OUTPUT***\n\n");
  fflush(NULL);

  /* Cleanup */
  for (i = 0; i < num_threads; i++) {
    random_ints_destroy(thread_args[i].ints);
  }
  generic_store_destroy(master);

  reset_counts();

  return;

}

unsigned int threads_working = 0;
int64_t ops_remaining=0;

void *fixed_benchmark_entrypoint(void *arg){
  char *ret_string;
  bool trans_merged = true;
  int num_puts = 0;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  pthread_cond_t *bench_cond;
  pthread_mutex_t *bench_mutex;
  void *master;
  void *worker;
  void *trans = NULL;
  int i = -1234;  //get rid of gcc warning
  int rc, starting_ops;
  int my_completed_ops = 0;
  int ret = 0, conflicts = 0, commits = 0;
  random_ints *ints;

  /* setup thread */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;
  starting_ops = thread_args->starting_ops;
  bench_cond = thread_args->bench_cond;
  bench_mutex = thread_args->bench_mutex;
  worker = generic_worker_create(master);
  ints = thread_args->ints;

  int my_num = thread_args->my_id;
  int key_count = 0;
  int offset = starting_ops/(thread_args->num_threads)*my_num;


  if (!bench_cond || !bench_mutex) {
    kp_die("got a null argument: bench_cond=%p, bench_mutex=%p\n",
        bench_cond, bench_mutex);
  }

  //eval_debug("thread_args: slam_local=%s (use single key), split_keys=%s "
  //    "(use partitioned keys)\n", thread_args->slam_local ? "true" : "false",
  //    thread_args->split_keys ? "true" : "false");
  
  /* Use the lock to start things off */
  rc = pthread_mutex_lock(bench_mutex);
  if (rc != 0) {
    kp_die("pthread_mutex_lock() returned error %d\n", rc);
  }
  trans = generic_trans_start(worker); //This doesn't need to be locked...
  rc = pthread_mutex_unlock(bench_mutex);
  if(rc != 0){
    kp_die("pthread_mutex_unlock() returned error %d\n", rc);
  }


  for(i = 0; i < max_kv_size; i ++) {
    if (ops_remaining <= 0) {
      break;
    }
    i = starting_ops - ops_remaining;
    ops_remaining--;

    /* Do work */
    if(thread_args->slam_local)
      rc = random_on_single_key(worker, trans, i, temp_keys[0], ints);
    else if(thread_args->split_keys)
      rc = random_on_partitioned_keys(worker, trans, i, 
                                      offset, &key_count, ints);
    else
      rc = create_random_request(worker, trans, i, ints);
    if (rc == -1) {
      kp_log(log_file,
             "create_random_request() returned error, looping again\n");
      continue;
    }else if (rc == 0 || rc == 1) {
      num_puts++;
      trans_merged = false;
    }
    my_completed_ops++;

    /* Merge every once in a while: */
    if ((num_puts % merge_every) == 0 \
        && (!trans_merged)) {
      ret = generic_trans_end(worker, trans);
      if (ret)
        conflicts++;
      commits++;
      trans_merged = true;
      trans = generic_trans_start(worker);
    }
  }

  /* Finish up by merging in things*/
  if(! trans_merged) {
    ret = generic_trans_end(worker, trans);
    if (ret)
      conflicts++;
    commits++;
  }

  rc = pthread_mutex_lock(bench_mutex); /* lock again in case we need to sig*/
  if(rc != 0){
    kp_die("pthread_mutex_lock() returned error %d\n", rc);
  }

  threads_working--;
  rc = pthread_cond_signal(bench_cond);
  if(rc != 0){
    kp_die("pthread_cond_signal() returned error %d\n", rc);
  }

  rc = pthread_mutex_unlock(bench_mutex);
  if(rc != 0){
    kp_die("pthread_mutex_unlock() returned error %d\n", rc);
  }

  /* Destroy (or fake-destroy) a worker */
  generic_worker_destroy(worker);
  printf("\t Conflicts in worker thread: %d (of %d commits)\n",
      conflicts, commits);
    //TODO: return this value to main thread, have it total the conflicts
    //  from all workers.

  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
  if (!ret_string)
    kp_die("malloc(ret_string) failed\n");
  snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
  if(K_DEBUG)
    printf("thread %lu returning string=%s\n", tid, ret_string);
  return (void *) ret_string;
}


void fixed_generic_benchmark(int num_threads, char *bench_string,
                             bool slam_local, bool split_keys,
                             bool measurement_thread){
  int rc, i = 0;
  void *ret_thread;
  int cpu;
  pthread_attr_t attr;
  pthread_t threads[MAX_THREADS];
  benchmark_thread_args thread_args[MAX_THREADS];
  pthread_cond_t *bench_cond;
  pthread_mutex_t *bench_mutex;

  /* Check args */
  if(num_threads <= 0){
    kp_die("invalid num_threads=%d\n", num_threads);
  }
  kp_debug("pid=%d, num_threads=%d\n", getpid(), num_threads);


  /* Create the master and one local worker*/
  void * master = generic_store_create();
  void * worker = generic_worker_create(master);
 
  /* Allocate the mutex and condition variable. kp_mutex_create() is just
   * a helper function that uses default attributes. */
  rc = kp_mutex_create("bench_mutex", &bench_mutex);
  if (rc != 0 || !bench_mutex) {
    kp_die("kp_mutex_create() failed\n");
  }
  bench_cond = (pthread_cond_t *)malloc(sizeof(pthread_cond_t));
  if (!bench_cond) {
    kp_die("malloc(pthread_cond_t) failed\n");
  }
  rc = pthread_cond_init(bench_cond, NULL);  //default attributes
  if (rc != 0) {
    kp_die("pthread_cond_init() failed\n");
  }

  /* sieze the lock so none of the threads can start */
  rc = pthread_mutex_lock(bench_mutex);
  if(rc != 0){
    kp_die("pthread_mutex_lock() returned error %d\n", rc);
  }

   /* Pre-allocate random numbers for each thread, _before_ starting any
   * of them: */
  for (i = 0; i < num_threads; i++) {
    rc = random_ints_create(&(thread_args[i].ints), random_int_count);
    if (rc != 0) {
      kp_error("random_ints_create() failed\n");
      return;
    }
  }
 /* Create a single thread */
  for (i=0; i < num_threads; i++){
    /* Create attributes */
    rc = pthread_attr_init(&attr);
    if(rc != 0)
      kp_die("pthread_attr_init() returned error=%d\n", rc);
    
    /* Set CPU and affinity */
    cpu = CPU_OFFSET + (i % NUM_CPUS);
    CPU_ZERO(&(thread_args[i]).cpu_set);
    CPU_SET(cpu, &(thread_args[i]).cpu_set);
    rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t),
                                     &((thread_args[i]).cpu_set));
    if(rc != 0)
      kp_die("pthread_attr_setaffinity_np() returned error=%d\n", rc);
    #ifdef PRINT_CPUS
    kp_print("pinned new thread to CPU=0x%X\n", cpu);
    #endif

    /* Set the arguments and spawn */
    thread_args[i].my_id = i;
    thread_args[i].slam_local = slam_local;
    thread_args[i].split_keys = split_keys;
    thread_args[i].master = master;
    thread_args[i].num_threads = num_threads;
    thread_args[i].starting_ops = BENCH_EXTEND*iterations;
    thread_args[i].bench_mutex = bench_mutex;
    thread_args[i].bench_cond = bench_cond;

    if(measurement_thread && i - num_threads -1)
      rc = pthread_create(&threads[i], &attr, &measurement_thread_entrypoint, 
                          (void *)(&(thread_args[i])));
    else{
      rc = pthread_create(&threads[i], &attr, &fixed_benchmark_entrypoint, 
                          (void *)(&(thread_args[i])));
    }
    if(rc != 0) {
      if (rc == 22) {
        kp_error("pthread_create() returned error=%d: check that NUM_CPUS "
            "(%d) is not greater than number of physical cores!\n",rc,NUM_CPUS);
      }
      kp_die("pthread_create() returned error=%d\n", rc);
    }
    
    /* Destroy the attributes */
    rc = pthread_attr_destroy(&attr);
    if(rc != 0)
      kp_die("pthread_attr_destroy() returned error=%d\n", rc);
  }

  /*=============== MASTER ACTIONS ==================== */
  
  /* Set the ops for threads */
  if(measurement_thread)
    threads_working = num_threads -1;
  else
    threads_working = num_threads;
  ops_remaining = BENCH_EXTEND*iterations;
  reset_thpt();

  /* Set timing */
  struct timeval *start = malloc(sizeof(struct timeval));
  if (!start)
    kp_die("malloc(start) failed\n");
  struct timeval *end = malloc(sizeof(struct timeval));
  if (!end)
    kp_die("malloc(end) failed\n");

  gettimeofday(start, NULL);

  /* wait for signal*/
  while (threads_working > 0) {
    rc = pthread_cond_wait(bench_cond, bench_mutex);
    if(rc != 0){
      kp_die("pthread_cond_wait() returned error %d\n", rc);
    }
  }

  int saved_puts = thpt_numputs;
  int saved_gets = thpt_numgets;
  int saved_dels = thpt_numdels;
  int total_ops = saved_puts + saved_gets + saved_dels;
  gettimeofday(end, NULL);

  int usec, sec, time_usec;
  /* Figure out timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;

  /* Print results */
  printf("For Fixed %s Benchmark (random+latency):"
         "\n\t benchmark cost %d:%d (sec:usec)"
         "= %d usecs \n",
         bench_string, sec, usec, time_usec);
  printf("\tAt a r-w-u proportion of %.2f-%.2f-%.2f...\n",
         (1-put_probability)*100,
         (put_probability * (1-update_probability))*100,
         (put_probability * update_probability)*100);
  printf("\t put throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_puts/(float)time_usec, 
         1000000*(float)saved_puts/(float)time_usec*value_size);
  printf("\t get throughput of %f ops/sec (%f bytes/sec)\n",
         1000000*(float)saved_gets/(float)time_usec,
         1000000*(float)saved_gets/(float)time_usec*value_size);
  printf("\t total throughput of %f ops/sec\n\n",
         1000000*(float)total_ops/(float)time_usec);

  rc = pthread_mutex_unlock(bench_mutex);
  if(rc != 0){
    kp_die("pthread_mutex_unlock() returned error %d\n", rc);
  }
  /* Get that worker back! */
  for( i = 0; i < num_threads-1; i++){
    rc = pthread_join(threads[i], &ret_thread);
    if(rc != 0)
      kp_die("pthread_join() returned error=%d\n", rc);
    if(ret_thread)
      free(ret_thread);
  }


  /* Cleanup */
  generic_worker_destroy(worker);
  generic_store_destroy(master);
  for (i = 0; i < num_threads; i++) {
    random_ints_destroy(thread_args[i].ints);
  }

  reset_counts();

  return;


}


void fixed_global_benchmark(int num_threads){
  fixed_generic_benchmark(num_threads, "split_keys", false, true, false);
}

void fixed_with_measurement_thread(int num_threads){
  fixed_generic_benchmark(num_threads, "split_keys", false, true, true);
}


void fixed_local_benchmark(int num_threads){
  fixed_generic_benchmark(num_threads, "single_key", true, false, false);
}


void fixed_read_benchmark(int num_threads){
  /* set probs */
  put_probability = .05;
  update_probability = .7;

  /* run benchmark */
  fixed_generic_benchmark(num_threads, "read-heavy", false, false, false);
}
void fixed_write_benchmark(int num_threads){
  /* set probs */
  put_probability = .15;
  update_probability = .125;

  /* run benchmark */
  fixed_generic_benchmark(num_threads, "write-heavy", false, false, false);
}
void fixed_update_benchmark(int num_threads){
  /* set probs */
  put_probability = .15;
  update_probability = .875;

  /* run benchmark */
  fixed_generic_benchmark(num_threads, "update-heavy", false, false, false);
}



/* Packages up single threaded evaluations so we can use it from within
   a single worker setup */
void *little_latency_wrapper(void *arg){
  char *ret_string;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  void *master;
  void *worker;

  /* setup thread */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;

  /* Do work */
  worker = generic_worker_create(master);
  latency_evaluation(iterations, worker);
  generic_worker_destroy(worker);
  reset_counts();

  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
  if (!ret_string)
    kp_die("malloc(ret_string) failed\n");
  snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
  if(K_DEBUG)
    printf("thread %lu returning string=%s\n", tid, ret_string);
  return (void *) ret_string;
}

void *little_split_latency_wrapper(void *arg){
  char *ret_string;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  void *master;
  void *worker;

  /* setup thread */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;

  /* Do work */
  worker = generic_worker_create(master);
  split_latency_evaluation(iterations, worker);
  generic_worker_destroy(worker);
  reset_counts();

  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
  if (!ret_string)
    kp_die("malloc(ret_string) failed\n");
  snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
  if(K_DEBUG)
    printf("thread %lu returning string=%s\n", tid, ret_string);
  return (void *) ret_string;
}

void *little_workload_wrapper(void *arg){
  char *ret_string;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  void *master;
  void *worker;
  random_ints *ints;

  /* setup thread */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;
  ints = thread_args->ints;

  /* Do work */
  worker = generic_worker_create(master);
  workload_evaluation(iterations, worker, ints);
  generic_worker_destroy(worker);
  reset_counts();

  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
  if (!ret_string)
    kp_die("malloc(ret_string) failed\n");
  snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
  if(K_DEBUG)
    printf("thread %lu returning string=%s\n", tid, ret_string);
  return (void *) ret_string;
}

void *little_kvsize_wrapper(void *arg){
  char *ret_string;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  void *master;
  void *worker;

  /* setup thread */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;

  /* Do work */
  worker = generic_worker_create(master);
  local_keyvalue_size(iterations, worker);
  generic_worker_destroy(worker);
  reset_counts();

  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
  if (!ret_string)
    kp_die("malloc(ret_string) failed\n");
  snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
  if(K_DEBUG)
    printf("thread %lu returning string=%s\n", tid, ret_string);
  return (void *) ret_string;
}

void single_worker_test(void){
  int rc, i = 0;
  void *ret_thread;
  int cpu;
  pthread_attr_t attr;
  pthread_t thread;
  benchmark_thread_args thread_args;
  void *master;
  random_ints *ints;
  
  /* Setup for everybody: don't spawn yet */
  /* Set CPU */
  cpu = CPU_OFFSET + (i % NUM_CPUS);
  CPU_ZERO(&(thread_args.cpu_set));
  CPU_SET(cpu, &(thread_args.cpu_set));

  /* Create Attributes */
  rc = pthread_attr_init(&attr);
  if(rc != 0)
    kp_die("pthread_attr_init() returned error=%d\n", rc);

  /* Set affinity */
  rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t),
                                   &(thread_args.cpu_set));
  if(rc != 0)
    kp_die("pthread_attr_setaffinity_np() returned error=%d\n", rc);
#ifdef PRINT_CPUS
  kp_print("pinned new thread to CPU=0x%X\n", cpu);
#endif

  printf("\n*** BEGINING SINGLE THREADED TESTS***\n\n");
  /*========================================= */
  /* Create the master for latency and memcpy*/
  master = generic_store_create();
  thread_args.master = master;
  rc = random_ints_create(&ints, random_int_count);
  if (rc != 0) {
    kp_error("random_ints_create() failed\n");
    return;
  }
  thread_args.ints = ints;

  /* Spawn the thread */
  rc = pthread_create(&thread, &attr, &little_latency_wrapper,
                      (void *)(&thread_args));
  if(rc != 0)
    kp_die("pthread_create() returned error=%d\n", rc);

  /* Get that worker back and cleanup! */
  rc = pthread_join(thread, &ret_thread);
  if(rc != 0)
    kp_die("pthread_join() returned error=%d\n", rc);
  if(ret_thread)
    free(ret_thread);
  generic_store_destroy(master);

  /*========================================= */
#if 0  /* Create the master for workload*/
  master = generic_store_create();
  thread_args.master = master;

  /* Spawn the thread */
  rc = pthread_create(&thread, &attr, &little_workload_wrapper, 
                      (void *)(&thread_args));
  if(rc != 0)
    kp_die("pthread_create() returned error=%d\n", rc);

  /* Get that worker back and cleanup! */
  rc = pthread_join(thread, &ret_thread);
  if(rc != 0)
    kp_die("pthread_join() returned error=%d\n", rc);
  if(ret_thread)
    free(ret_thread);
  generic_store_destroy(master);
#endif
  /*========================================= */
#if 0
  /* Create the master for keyvalue size*/
  master = generic_store_create();
  thread_args.master = master;

  /* Spawn the thread */
  rc = pthread_create(&thread, &attr, &little_kvsize_wrapper,
                      (void *)(&thread_args));
  if(rc != 0)
    kp_die("pthread_create() returned error=%d\n", rc);

  /* Get that worker back and cleanup! */
  rc = pthread_join(thread, &ret_thread);
  if(rc != 0)
    kp_die("pthread_join() returned error=%d\n", rc);
  if(ret_thread)
    free(ret_thread);
  generic_store_destroy(master);
#endif
  /*========================================= */
  printf("\n*** CONCLUDING SINGLE THREADED TESTS***\n\n");

  rc = pthread_attr_destroy(&attr);
  if(rc != 0)
    kp_die("pthread_attr_destroy() returned error=%d\n", rc);
  return;
}

void base_number_test(void){
  int rc, i = 0;
  void *ret_thread;
  int cpu;
  pthread_attr_t attr;
  pthread_t thread;
  benchmark_thread_args thread_args;
  void *master;
  
  /* Setup for everybody: don't spawn yet */
  /* Set CPU */
  cpu = CPU_OFFSET + (i % NUM_CPUS);
  CPU_ZERO(&(thread_args.cpu_set));
  CPU_SET(cpu, &(thread_args.cpu_set));

  /* Create Attributes */
  rc = pthread_attr_init(&attr);
  if(rc != 0)
    kp_die("pthread_attr_init() returned error=%d\n", rc);

  /* Set affinity */
  rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t),
                                   &(thread_args.cpu_set));
  if(rc != 0)
    kp_die("pthread_attr_setaffinity_np() returned error=%d\n", rc);
#ifdef PRINT_CPUS
  kp_print("pinned new thread to CPU=0x%X\n", cpu);
#endif

  printf("\n*** BEGINING BASE NUMBER TESTS***\n\n");
  /*========================================= */
  /* Create the master for latency and memcpy*/
  master = generic_store_create();
  thread_args.master = master;

  /* Spawn the thread */
  rc = pthread_create(&thread, &attr, &little_latency_wrapper,
                      (void *)(&thread_args));
  if(rc != 0)
    kp_die("pthread_create() returned error=%d\n", rc);

  /* Get that worker back and cleanup! */
  rc = pthread_join(thread, &ret_thread);
  if(rc != 0)
    kp_die("pthread_join() returned error=%d\n", rc);
  if(ret_thread)
    free(ret_thread);
  generic_store_destroy(master);

  /*========================================= */
  printf("\n*** CONCLUDING BASE NUMBER TESTS***\n\n");

  rc = pthread_attr_destroy(&attr);
  if(rc != 0)
    kp_die("pthread_attr_destroy() returned error=%d\n", rc);
  return;
}


/* Sets this process' affinity to run on only a single CPU. NUM_CPUS
 * must be set before calling this function.
 *
 * I'm not sure what impact this has on a multi-threaded process: will
 * it force all of the process' threads onto a single CPU? It seems
 * like we would have noticed this already if this were true, but
 * who knows.
 */
void set_process_affinity(int cpu)
{
  int ret;
  cpu_set_t mask;

  if (cpu > NUM_CPUS) {
    kp_die("cpu %d is too high; NUM_CPUS = %d\n", cpu, NUM_CPUS);
  } else if (cpu < 0) {
    kp_die("invalid cpu: %d\n", cpu);
  }

  CPU_ZERO(&mask);
  CPU_SET(cpu, &mask);  //add CPU 1 to mask

  ret = sched_setaffinity(0, sizeof(cpu_set_t), &mask);
  if (ret != 0) {
    printf("ERROR: sched_setaffinity() returned %d\n", ret);
    abort();
  }

#ifdef PRINT_CPUS
  kp_print("pinned this PROCESS to CPU=0x%X\n", cpu);
#endif
}

/* Prints usage and then exits with return code -1. */
void usage(char *progname)
{
  printf("USAGE: %s [--pause] [--delay=<secs>] [--kpvm-dram] [--leveldb-disk] "
         "[--leveldb-ramdisk] [--base] <cpus> <iters> <ksize> <vsize> "
         "<merge_every> <put_prob> <update_prob> <threads> \n", progname);
  printf("  If --pause is used, the evaluation will pause after the initial\n");
  printf("    key and value setup and wait for the user to press Enter.\n");
  printf("  --delay will delay the evaluation start by the number of seconds\n");
  printf("  If threads is = 1, only single-threaded tests will be run.\n");
  printf("  Store: kpvm=0, leveldb=2, cassandra=1.\n");
  exit(-1);
}

/* Parses the arguments passed to the program and performs basic validity
 * checks, which right now consist of:
 *   num_threads >= 1
 *   delay >= 0
 * If invalid arguments or not enough arguments are passed, then this
 * function will exit(). If this function returns, *num_threads and 
 * *pause_before_start will be set with their corresponding arguments;
 * all other args are currently global variables.
 */
void parse_arguments(int argc, char *argv[], int *num_threads,
                     bool *pause_before_start, int *delay,
                     bool *base)
{
  int c;
  char *endptr;
  int option_index;
  static int pause_flag = 0;
  static int base_flag = 0;
  static struct option long_options[] =
  {
    /* If third member ("flag") is set, then getopt_long() returns 0
     * and the flag variable is set to the fourth member ("val") if
     * the option is found. The flag variable is left unchanged if the
     * option is not found.
     */
    {"pause", no_argument, &pause_flag, 1},
    {"delay", required_argument, NULL, 'd'},
    {"base", no_argument, &base_flag, 1},
    {"kpvm-dram", no_argument, &WHICH_KEY_VALUE_STORE, 0},
    {"leveldb-disk", no_argument, &WHICH_KEY_VALUE_STORE, 20},
    {"leveldb-ssd", no_argument, &WHICH_KEY_VALUE_STORE, 21},
    {"leveldb-ramdisk", no_argument, &WHICH_KEY_VALUE_STORE, 22},
    {"cassandra-disk", no_argument, &WHICH_KEY_VALUE_STORE, 10},
    {"cassandra-ssd", no_argument, &WHICH_KEY_VALUE_STORE, 11},
    {"cassandra-ramdisk", no_argument, &WHICH_KEY_VALUE_STORE, 12},
    {0, 0, 0, 0}
  };

  /* Get optional arguments: */
  option_index = 0;
  while (1) {
    /* Set optstring (third arg to getopt_long) to accept short options
     * as well as long options.
     * See getopt_long(3).
     */
    c = getopt_long(argc, argv, "pd:", long_options, &option_index);
    if (c == -1) {
      break;  //no more - or -- options
    }

    switch(c) {
    case 0:  //getopt_long() got a flag option
      if (long_options[option_index].flag != 0) {
        kp_debug("got option %s, set its flag to %d\n",
            long_options[option_index].name,
            long_options[option_index].val);
      } else {
        kp_error("Should never reach this code (case 0)\n");
      }
      break;  //getopt_long() already set the flag for this option

    case 'p':
      kp_debug("case 'p': setting pause_flag to 1\n");
      pause_flag = 1;
      break;

    case 'd':
      kp_debug("case 'd': setting delay to optarg=%s\n", optarg);
      *delay = strtol(optarg, &endptr, 10);  //atoi doesn't detect errors
      if (*endptr != '\0') {
        kp_error("invalid delay value: %s\n", optarg);
        usage(argv[0]);
      }
      break;

    default:  //getopt_long() may return ':' or '?' for unrecognized/missing options
      kp_error("unrecognized option or missing required option value\n");
      usage(argv[0]);
    }
  }
 
  /* Convert int flags into bools... */
  if (pause_flag == 0) {
    *pause_before_start = false;
  } else {
    *pause_before_start = true;
  }
  if (base_flag == 0) {
    *base = false;
  } else {
    *base = true;
  }
  if(WHICH_KEY_VALUE_STORE >=10){
    WHICH_STORAGE_PLATFORM = WHICH_KEY_VALUE_STORE%10;
    WHICH_KEY_VALUE_STORE = WHICH_KEY_VALUE_STORE/10;
  }

  kp_debug("after processing, *pause_before_start is %s, delay is %d\n",
      *pause_before_start ? "true" : "false", *delay);

  /* Now, process the remaining, non-option arguments. getopt_long() has
   * set optind to the index of the first non-option argument in argv.
   * NOTE: "By default, getopt() permutes the contents of argv as it scans,
   * so that eventually all the non-options are at the end."
   * There should be 9 arguments remaining in argv:
   */
  kp_debug("after option processing, optind=%d, argc=%d\n", optind, argc);
  if (argc - optind != 8) {
    kp_error("wrong number of arguments\n");
    usage(argv[0]);
  }
  
  NUM_CPUS = atoi(argv[optind]);
  iterations = atoi(argv[optind+1]);       //1
  key_size = atoi(argv[optind+2]);       //2
  value_size = atoi(argv[optind+3]);     //3
  merge_every = atoi(argv[optind+4]);    //4
  /* TODO add capability to alter these: */
  put_probability = .1;                  //5 (optind+5)
  update_probability = .7;               //6 (optind+6)
  *num_threads = atoi(argv[optind+7]);   //7

  kp_debug("got arguments: CPUS=%d,iterations=%d, key_size=%d, value_size=%d, "
           "merge_every=%d, put_probability=%f, update_probability=%f, "
           "num_threads=%d\n", NUM_CPUS,
           iterations, key_size, value_size, merge_every, put_probability,
           update_probability, *num_threads);

  /* Validity checking: */
  if (NUM_CPUS < 1) {
    kp_error("num_cpus (%d) is less than 1\n", NUM_CPUS);
    usage(argv[0]);
  }
  if (*num_threads < 1) {
    kp_error("num_threads (%d) is less than 1\n", *num_threads);
    usage(argv[0]);
  }
  if (*delay < 0) {
    kp_error("delay (%d) is less than 0\n", *delay);
    usage(argv[0]);
  }
  if (WHICH_KEY_VALUE_STORE < 0 || WHICH_KEY_VALUE_STORE > 2){
    kp_error("unrecognized (%d) key value store \n", WHICH_KEY_VALUE_STORE);
    usage(argv[0]);
  }
}

int main(int argc, char *argv[])
{
  int num_threads = 0;
  int rc = 0;
  int i;
  unsigned int max_kv_size_mult;
  bool pause_before_start = false;
  bool base = false;
  int delay = 0;

  srandom(time(NULL));

  parse_arguments(argc, argv, &num_threads, &pause_before_start, &delay, &base);
  kp_debug("after parse_arguments(), num_threads=%d, pause_before_start=%s, "
      "delay=%d, do_conflict_detection=%s\n", num_threads,
      pause_before_start ? "true" : "false", delay,
      do_conflict_detection ? "true" : "false");

  /*--------------------------------------------------------------*/
  /* Do global allocations. The maximum size we need for the key value
   * store is the number of puts that we may do to the store;
   * 
   * puts in latency = (#iters)(1+1+MULT_FACTOR+MULT_FACTOR+1);
   * puts in workload = #iters
   * puts in local_size = # iters
   * puts in worker_thread = MT_EXTEND* # iters
   * puts in worker_main = puts in latency + puts in workload
   * total threaded puts = #iters(1 + 
   *                              3+ MULT_FACTOR*2+
   *                              MT_EXTEND*NUM_THREADS)
   * benchmarks are always BENCCH_EXTEND * #iters
   */
  //  max_kv_size_mult = ((4 + (MULT_FACTOR*2) + (MT_EXTEND*num_threads)) >
  //                      (BENCH_EXTEND))? (4 + (MULT_FACTOR*2) + (MT_EXTEND*num_threads)) : BENCH_EXTEND;
  max_kv_size_mult = 1+MULT_FACTOR*2;
  max_kv_size = iterations * max_kv_size_mult;
  local_expected_max_keys = merge_every+1;
    /* For some reason, when local_expected_max_keys is set exactly to
     * merge_every, we get a few of these messages:
     *   VECTOR: 3074344656: v->size hit v->count = 100; resizing!!!
     * It seems like there's only 3-4 of them each run; I'm not sure why,
     * maybe there is a small bug in how the number of merge operations
     * is counted. But anyway, we add a fudge factor here. */
  kp_print("Using %u for local hash table size (same as merge frequency), "
      "%u for master hash table size\n", local_expected_max_keys,
      max_kv_size);

  /* We're allocating 5 arrays of max_kv_size here, where the size of
   * each entry is either an int or a pointer, so probably 4 bytes for
   * each entry. Additionally, two of the arrays are filled with keys
   * (key_size+1) and values (value_size+1), and a third array
   * (dest_values) will be filled with values during the get test.
   * If we run the malloc + memcpy tests without "free_me" set to true,
   * then we'll additionally fill the second_values array with values
   * and the second_keys array with keys (but this will not happen at
   * the same time as dest_values is used in the get test).
   * So, the maximum amount of memory (in bytes) that we'll allocate
   * during our evaluation run (excepting a few small structs that are
   * allocated, e.g. the timers) is:
   *     5 * max_kv_size * 4                +
   *     2 * max_kv_size * (key_size + 1)   +
   *     2 * max_kv_size * (value_size + 1)
   * Or: max_kv_size * (23 + 2*key_size + 2*value_size)
   * With key_size = 32 and val_size = 128, the inner term is 343 bytes.
   *   max_kv_size = 1,000,000 -> 343000000 bytes = 327.11 MB, which
   *     should fit in memory...
   *   max_kv_size = 10,000,000 -> 3430000000 bytes = 3.19 GB, which
   *     may not fit in memory!
   */
  unsigned int total_allocation_size =
    max_kv_size * (2*sizeof(char*) + 2*sizeof(void*) + sizeof(int) +
        2*(key_size + 1) + 2*(value_size + 1));
  kp_print("Total memory allocation size (just evaluation, not kv stores "
      "themselves) could be up to %u bytes (%u MB)\n",
      total_allocation_size, total_allocation_size / (1<<20));

  /*  if (total_allocation_size / (1<<20) > 512) {
    kp_warn("Check that this allocation size will fit in memory!!!\n");
    }*/
  printf("Creating %u (iterations * %u) random keys and "
      "values...\n", max_kv_size, max_kv_size_mult);

  /*--------------------------------------------------------------*/
#ifdef KP_EVAL_LOG
  log_file = fopen(log_file_name, "a");
  if(log_file != NULL){
    kp_log(log_file, "------ New Run ------\n");
  }
  else{
    exit(-1);
  }
#endif
  /* Do allocation/creation of random keys and values */
  temp_keys = malloc(max_kv_size*sizeof(char*));
  if (!temp_keys)
    kp_die("malloc(temp_keys) failed\n");


  temp_value = malloc(value_size+1);
  if (!temp_value)
    kp_die("malloc(temp_value) failed\n");
  rc = create_random_value(value_size, temp_value);
  dest_values = malloc(max_kv_size*sizeof(void*));
  if (!dest_values)
    kp_die("malloc(dest_values) failed\n");
  
  for(i = 0; i < max_kv_size; i++){
    temp_keys[i] = malloc(key_size+1);
    if (!temp_keys[i])
      kp_die("malloc(temp_keys[%d]) failed\n", i);
    rc = create_random_key(key_size, temp_keys[i]);
  }
  random_int_count = iterations * MT_EXTEND * 3;
  printf("Created %u random keys and values\n", max_kv_size);
  printf("Will create %u random integers for each child thread\n",
      random_int_count);

  printf("Now starting evaluation for key-value store: %s\n",
     WHICH_KEY_VALUE_STORE == 0 ? "kp_kvstore" :
     WHICH_KEY_VALUE_STORE == 1 ? "Cassandra" : 
     WHICH_KEY_VALUE_STORE == 2 ? "LevelDB" : "unknown!");
  printf("On %d cores\n", NUM_CPUS);
  printf("Running on storage platform: %s\n",
     WHICH_STORAGE_PLATFORM == 0 ? "disk" :
     WHICH_STORAGE_PLATFORM == 1 ? "ssd" : 
     WHICH_STORAGE_PLATFORM == 2 ? "ramdisk" : "unknown!");
         


  /*--------------------------------------------------------------*/
  /* Run either single-threaded evaluation or multi-threaded evaluation,
   * depending on num_threads arg:
   */
  if (pause_before_start) {
    printf("\npid of evaluation process: %d\n", getpid());
    printf("PRESS ENTER TO BEGIN EVALUATION RUN");
    fgetc(stdin);  //ignore input, just wait for Enter
    printf("\n");
  }

  if (delay > 0) {
    printf("\nSLEEPING FOR %d SECONDS BEFORE BEGINNING EVALUATION RUN\n",
        delay);
    sleep(delay);
  }
  if(base){
    printf("Getting base numbers\n");
    base_number_test();
  }  else{
    printf("Starting single-threaded tests\n");
    //single_worker_test();
    printf("Starting multi-threaded tests: num_threads=%d, NUM_CPUS=%d\n",
           num_threads, NUM_CPUS);
    ramp_up_threads(num_threads);
    //    fixed_generic_benchmark(num_threads, "generic", false, false, false);
    //    fixed_read_benchmark(num_threads);
    //    fixed_write_benchmark(num_threads);
    //    fixed_update_benchmark(num_threads);
    //    fixed_global_benchmark(num_threads);
    //    fixed_local_benchmark(num_threads);
    //    fixed_with_measurement_thread(num_threads);
    printf("***CONCLUDING THREAD EVALUATION***\n");
  }
  /*--------------------------------------------------------------*/
#ifdef KP_EVAL_LOG
  fclose(log_file);
#endif
  /* Free global allocations */

  /* Free random keys and values */
  free(temp_keys);
  free(temp_value);
  //  free(temp_values);
  free(dest_values);


  /* end */
  if(rc)
    printf("We should probably check eval return values. Oops.\n");

  return 0;
}

/* Editor modelines  -  http://www.wireshark.org/tools/modelines.html
 *
 * Local variables:
 * c-basic-offset: 2
 * tab-width: 2
 * indent-tabs-mode: nil
 * End:
 *
 * vi: set shiftwidth=2 tabstop=2 expandtab:
 * :indentSize=2:tabSize=2:noTabs=true:
 */
