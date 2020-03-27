/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: nil
 * vi: set shiftwidth=2 tabstop=2 expandtab:
 * :indentSize=2:tabSize=2:noTabs=true:
 *
 * Katelin Bailey & Peter Hornyack
 * 8/28/2011
 */

//#define CASSANDRA_INSTALLED
//#define PIN_INSTALLED

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sched.h>
#include <getopt.h>
#include <assert.h>
#include <errno.h>

#include "../kp_macros.h"
#include "../kp_common.h"
#include "../../include/kp_kv_local.h"
#include "../../include/kp_kv_master.h"
#include "../PIN_Hooks.h"

bool print_stats = false;
bool do_conflict_detection = true;
bool use_durable_ldb = true;
int WHICH_STORAGE_PLATFORM;
int WHICH_KEY_VALUE_STORE;


/* Switch on which keyvalue store to use */
#define USE_KP_KV_STORE 0
#define USE_CASSANDRA 1
#define USE_LEVELDB 2
#define USE_MEMCACHED 3
#define USE_MASSTREE 4
#define USE_NOOP 5

#define USE_DISK 0
#define USE_SSD 1
#define USE_RAMDISK 2

#define DISK_PATH "/home/katelin/leveldb_client_name"
#define SSD_PATH "/mnt/pmfs/leveldb_client_name"
#define RAMDISK_PATH "/mnt/pmfs/leveldb_client_name"

/* define PRINT_CPUS to print out which CPU each thread is pinned to. */
//#define PRINT_CPUS

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

/* Set to false to use just a single random value, rather than a bazillion
 * different random values.
 * For minimal memory overhead, set to false, false, true, true 
 */
bool use_random_values = false;
bool use_random_keys = false;
bool use_rand = false;
bool free_gotten_vals = true;
bool use_split_workload = false;
bool use_get_workload = false;
bool check_workload = false;


/* These two constants define the range of CPUs that the worker threads
 * will be placed on: [CPU_OFFSET, CPU_OFFSET + NUM_CPUS - 1]. If
 * the high end of that range is greater than the number of CPUs on
 * the system, who knows what will happen.
 */
int NUM_CPUS=2;
#define CPU_OFFSET 0
#define MAX_THREADS 256

#define K_DEBUG 0      //Print Debug messages
#define PREPOP_NUM 10000 ///Number of keys to prepopulate with
#define MULT_FACTOR 3  //For cheap operations, # of extra times to iterate
#define KPMT_SLEEP 2   //Seconds to sleep during ramp up threads
#define BENCH_EXTEND 5   //To ensure the threads last a while, do extra iters
#define USE_OTHER_TIMERS 0
#define RET_STRING_LEN 64

#define RET_OKAY 0
#define RET_ERROR -1
#define RET_PUT_OKAY 2
#define RET_PUT_ERROR -2
#define RET_UPD_OKAY 3
#define RET_UPD_ERROR -3
#define RET_DEL_OKAY 4
#define RET_DEL_ERROR -4
#define RET_GET_OKAY 5
#define RET_GET_ERROR -5



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
  long int *array;
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
  int my_id;
  random_ints *ints;
} benchmark_thread_args;

/* Structure to store all the timer info for various options, one run */
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
float put_probability = .1;          // Probability that next request is a put
float update_probability = .7;       // Prob. that next put is to existing val
float delete_probability = .01;       // Probability that the next request is a delete
uint64_t max_key;
//uint64_t UINT_64_ALMOST_MAX = 184467440737095u;
uint64_t UINT_64_ALMOST_MAX = 1844674407u;

char *log_file_name = "kpvm_logged_errors.out";
FILE *log_file;


/* Keep track of some keys so we can run random workload */
uint64_t num_keys = 0;                    // For an instance, num unique keys in set
int thpt_numputs = 0;
int thpt_numgets = 0;
int thpt_numdels = 0;
int thpt_numops = 0;

int total_conflicts = 0;
int total_commits = 0;
int total_error_count = 0;
int total_put_error_count = 0;
int total_upd_error_count = 0;
int total_del_error_count = 0;
int total_get_error_count = 0;

int wp_numinsert = 0;
int wp_numupdate = 0;
int wp_numget = 0;
int wp_numdel = 0;

/* Do some block allocations at initialization */
void *temp_value;
void *dest_value;

void **dest_values;                  // Generate a gazillion potential values 
void **temp_values;                  // Generate a gazillion potential values 
char **temp_keys;                    // Generate a gazillion potential keys 

unsigned int random_int_count = 0;
bool measurement_in_progress;

/* Timing Functions */
void start_timing(kp_timers *timers);
void stop_timing(kp_timers *timers);
void print_timing(const char *prefix, kp_timers *timers, int divisor);

/* Interactions with the stores, as generic functions*/
void *generic_store_create(int num_threads, int max_master_store);
void *generic_worker_create(void *master_store, int max_local_store);
void generic_store_destroy(void *store);
void generic_worker_destroy(void *store);
void *generic_trans_start(void *store);
int generic_trans_end(void *store,void *trans);
int generic_get(void *store, char *key, char **value, size_t* size);
int generic_put(void *store, void *trans,const char *key, const char *value, const size_t size);
int generic_delete(void *store, void *trans, char *key);

/* Bookkeeping functions */
void print_workload(void);
void reset_thpt(void);
void reset_counts(void);

/* Randomizing functions */
int biased_update_coin(random_ints *ints);
int biased_put_get_coin(random_ints *ints);
int pick_random_key(int input_num_keys, random_ints *ints);
int create_random_string(int size, char *key);
int create_random_key(int size, char *key);
int create_random_value(int size, void *value);
void fetch_key_from_index(int index, void** key);
void fetch_value_from_index(int index, void** value);
int random_ints_create(random_ints **ints, unsigned int count);
long int random_ints_next(random_ints *ints);
void random_ints_destroy(random_ints *ints);

/* Workload, microbenchmark, and request generation functions */
int random_on_single_key(void *store, void *trans, int i, char* key,
    random_ints *ints);
int random_on_partitioned_keys(void *store, void *trans, int i, 
                               int offset, int *partition_count,
                               int partition_limit, random_ints *ints);
int create_random_request(void *store, void *trans, int i, random_ints *ints);
int create_random_get(void *store, void *trans, int i, random_ints *ints);
int workload_evaluation(int num_iterations, void *store, random_ints *ints);
int individual_function_eval(int iteration_count, int factor, void *store,
                             void **trans, int op, char* opstring,
                             int clear_count_num, int mergenum,
                             bool randomized);
int latency_evaluation(int num_iterations, void* store);
int local_keyvalue_size(int num_iterations, kp_kv_local *kv);


/* Threaded workload */
void *worker_thread_entrypoint(void *arg);
void ramp_up_threads(int num_threads, char* bench_string);

/* Threaded macrobenchmarks */
void *fixed_benchmark_entrypoint(void *arg);
void fixed_generic_benchmark(int num_threads, char* bench_string,
                             bool slam_local);
void fixed_global_benchmark(int num_threads);
void fixed_local_benchmark(int num_threads);
void rampup_percent_benchmark(int num_threads, 
                              float put_prob, 
                              float up_prob,
                              float del_prob);
void fixed_percent_benchmark(int num_threads, 
                             float put_prob, 
                             float up_prob,
                             float del_prob);

/* Wrappers for single-threaded evaluations */
void *little_latency_wrapper(void *arg);
void *little_workload_wrapper(void *arg);
void *little_kvsize_wrapper(void *arg);
void single_worker_test(void);

/* utilities*/
void set_process_affinity();
void usage(char *progname);
void parse_arguments(int argc, char *argv[], int *num_threads,
                     bool *pause_before_start, int *delay);


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


/* ==================================================== 
     GENERIC STORE FUNCTIONS: PARSE TO API
   ==================================================== */
void *generic_store_create(int num_threads, int max_master_store){
  int rc = 0;
  kp_kv_master *master;
  void *store;
  unsigned long tid = pthread_self();
  #ifdef CASSANDRA_INSTALLED
  cassandra_node *node = NULL;
  #endif

  switch ( WHICH_KEY_VALUE_STORE ) {
  case USE_KP_KV_STORE:
    /* Use the max_kv_size as the size of the hash table: */
    if(max_master_store > 0)
      rc = kp_kv_master_create(&master, MASTER_CONSISTENCY_MODE, 
                               max_master_store,
                               do_conflict_detection, use_nvm_master);
    else
      rc = kp_kv_master_create(&master, MASTER_CONSISTENCY_MODE, 
                               max_kv_size,
                               do_conflict_detection, use_nvm_master);
    if(rc != 0){
      kp_die("thread_%lu: kp_kv_master_create() returned error=%d\n", tid, rc);
    }
    store = (void *) master;
    break;

  default:
    kp_die("Request to use unknown KeyValueStore\n");
    break;
  }
  return store;

}

/* Create (one of) the worker/node stores, based on a global */
void *generic_worker_create(void *master_store, int max_local_store) {
  int rc = 0;
  kp_kv_master *master;
  kp_kv_local *local;
  void *store;
  unsigned long tid = pthread_self();

  #ifdef KP_ASSERT
  if (local_expected_max_keys <= 0) {
    kp_die("unexpected local_expected_max_keys value: %zu\n",
           local_expected_max_keys);
  }
  #endif
  
  switch ( WHICH_KEY_VALUE_STORE ) {
  case USE_KP_KV_STORE:
    master = (kp_kv_master*) master_store;
    if(max_local_store > 0)
      rc = kp_kv_local_create(master, &local, 
                              max_local_store,
                              use_nvm_local);
    else
      rc = kp_kv_local_create(master, &local, 
                              local_expected_max_keys, 
                              use_nvm_local);

    if(rc != 0){
      kp_die("thread_%lu: kp_kv_local_create() returned error=%d\n", tid, rc);
    }
    store = (void *) local;
    break;

  default:
    kp_die("Request to use unknown KeyValueStore\n");
    break;
  }
  return store;
}

/* Destroy the global/master store */
void generic_store_destroy(void *store){
  int rc = 0;
  kp_kv_master *master;
  unsigned long tid = pthread_self();

  switch (WHICH_KEY_VALUE_STORE){
  case USE_KP_KV_STORE:
    master = (kp_kv_master *) store;
    kp_kv_master_destroy(master);
    break;

  default:
       kp_die("Request to use unknown KeyValueStore\n");
    break;
  }
}


/* Destroy the local/worker/node stores */
void generic_worker_destroy(void *store){
  kp_kv_local *local;
  int rc = 0;
  unsigned long tid = pthread_self();


  switch (WHICH_KEY_VALUE_STORE){
  case USE_KP_KV_STORE:
    local = (kp_kv_local *) store;
    kp_kv_local_destroy(&local);
    break;

  default:
       kp_die("Request to use unknown KeyValueStore\n");
    break;
  }
}

/* Start a transaction and store relevant info, ret pointer */
void *generic_trans_start(void *store){
  void *trans = NULL;

  if(!store){
    kp_error("generic_trans_start recieved null argument\n");
    return NULL;
  }


  switch ( WHICH_KEY_VALUE_STORE ) {
  case USE_KP_KV_STORE:
    /* We don't have a formal transaction begin, it's implicit */
    break;
  default:
    kp_die("Request to use unknown KeyValueStore\n");
    break;
  }

  return trans;
}

/* End transaction and commit anything that hasn't been pushed through
   Returns: 0 on success or error, 1 if there was a conflict. */
int generic_trans_end(void *store, void *trans){
  int rc, retval;
  kp_kv_local *kv;

  if(!store){
    kp_error("generic_trans_end recieved null argument\n");
    return 0;
  }

  retval = 0;

  switch ( WHICH_KEY_VALUE_STORE ) {
  case USE_KP_KV_STORE:
    /* Perform a _synchronous_ commit, for now: */
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

  default:
    kp_die("Request to use unknown KeyValueStore\n");
    break;
  }

  return retval;
}

/* Returns: 0 if put succeeded, -1 if put failed. */
int generic_put(void *store, void * trans, const char *key, const char *value, const size_t size){
  const char* myval = value;
  int rc, retval;
  uint64_t rc64;
  kp_kv_local *kv;

  retval = 0;
  #ifdef KP_ASSERT
  if (!key) {
    kp_die("key is NULL!\n");
  }
  #endif
  switch ( WHICH_KEY_VALUE_STORE ) {
  case USE_KP_KV_STORE:
    kv = (kp_kv_local *)store;
    //    printf("generic_put(): to key: %s\n", key);
    rc64 = kp_local_put(kv, key, value, size);
    if (rc64 == UINT64_MAX) {
      retval = -1;
      kp_die("kp_local_put failed\n");
      #ifdef KP_ASSERT
      kp_die("kp_local_put(%s, %s, %zu) failed\n", key, value, size);
      #endif
    }
    break;

  default:
    kp_die("Request to use unknown KeyValueStore\n");
    break;
  }

  //  thpt_numputs++;
  return retval;
}

/* Returns: 0 if the get succeeded, -1 if the get failed. */
int generic_get(void *store, char *key, char **value, size_t* size){
  int rc, retval;
  kp_kv_local *kv;
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
      }
      else if (rc == 1) {  //tombstone, may be expected
        if(K_DEBUG) {
          printf("DEBUG: kp_local_get() could not find key=%s\n",
                 key);
        }
      } else {  //anything else, not expected
        retval = -1;
        /* Because not all gets will succeed in multithreaded
           You can let me know, but don't fail out */
        kp_log(log_file, "kp_local_get(%s) failed (%d: %s)\n",
               key, rc, kp_retval_to_str(rc));
        kp_die("kp_local_get failed\n");
      }
    }
    break;

  default:
    kp_die("Request to use unknown KeyValueStore\n");
    break;
  }

  //  thpt_numgets++;
  return retval;
}

/* Returns: 0 if the delete succeeded, -1 if the delete failed. */
int generic_delete(void *store, void *trans, char *key){
  int rc, retval;
  kp_kv_local *kv;
  retval = 0;

  switch ( WHICH_KEY_VALUE_STORE ) {
  case USE_KP_KV_STORE:
    kv = (kp_kv_local *)store;
    rc = kp_local_delete_key(kv, key);
    if (rc != 0) {
      retval = -1;
      if (rc == 1) { //OBSELETE?, the store just ignores key-not-found
        kp_error("kp_local_delete_key(%s) returned key not found\n", key);
      } else {
        kp_error("kp_local_delete_key(%s) returned error: %d\n", key, rc);
        kp_die("kp_local_delete_key(%s) returned error: %d\n", key, rc);
      }
      #ifdef KP_ASSERT
      kp_die("kp_local_delete_key(%s) returned error: %d\n", key, rc);
      #endif
    }
    break;

  default:
    kp_die("Request to use unknown KeyValueStore\n");
    break;
  }

  //  thpt_numdels++;
  return retval;
}

void print_workload(void){
  int total = wp_numinsert + wp_numupdate + wp_numget + wp_numdel;


  if(total != 0){
    printf("WORKLOAD:\t");
    printf("insert: %d\t", (100*wp_numinsert)/total);
    printf("update %d\t", (100*wp_numupdate)/total);
    printf("get: %d\t", (100*wp_numget)/total);
    printf("delete: %d\n", (100*wp_numdel)/total);
  }
}

void reset_thpt(void){
  if (check_workload)
    print_workload();

  /* Reset the thpt counters */
  thpt_numputs = 0;
  thpt_numgets = 0;
  thpt_numdels = 0;
  thpt_numops = 0;

  /* Reset any timers and conflict counters */
  total_conflicts = 0;
  total_commits = 0;
  total_error_count = 0;
  total_put_error_count = 0;
  total_upd_error_count = 0;
  total_del_error_count = 0;
  total_get_error_count = 0;

  /* Reset the workload counters */
  wp_numinsert = 0;
  wp_numupdate = 0;
  wp_numget = 0;
  wp_numdel = 0;
}


void reset_counts(void){
  num_keys = 0;
  reset_thpt();
}


/* Uses a biased flip to determine if it's a put-new or put-old */
int biased_update_coin(random_ints *ints){
  long int r;
  r = random_ints_next(ints);

  float rd = (float)r/(float)RAND_MAX;
  if(rd > update_probability)
      return 0; // put
  else
    return 1;   // update
}

/* Uses a biased flip to determine if it's a put or a get */
int biased_put_get_coin(random_ints *ints){
  long int r;
  r = random_ints_next(ints)+1;

  float rd = ((float)r)/((float)RAND_MAX);
  //strictly less than, and greater or equal since random is 0-inclusive
  if( rd < delete_probability)
    return 2;
  else if (rd < put_probability)
    return 0;
  else
    return 1;
}

/* Pick any random key from those we've added (forget s/deletes/updates) */
int pick_random_key(int input_num_keys, random_ints *ints){
  long int r;
  int index;
  r = random_ints_next(ints);

  float rd = (float)r/(float)RAND_MAX ;
  index = rd*(input_num_keys-1);
  if(K_DEBUG)
    printf("(%d/%d)\n", index, input_num_keys);
  return rd;
}

/* Creates a random n-character null-terminated string */
int create_random_string(int size, char *key){
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
int create_random_key(int size, char *key){
  int rc = 0;
  rc = create_random_string(size, (char*) key);
  return rc;
}

/* Creates a random string of n bytes for a value */
int create_random_value(int size, void *value){
  int rc = 0;
  rc = create_random_string(size, (char*) value);
  return rc;
}

/* Either fetch the key from the stored ones, or just print
   out the index with appropriate size*/
void fetch_key_from_index(int index, void** key){
  if(use_random_keys)
    *key = (void*) temp_keys[index];
  else {
    uint64_t real_index = 0;
    // don't overrun the maximum number of keys we can represent
    if (index != 0)
      real_index = index % max_key;

    *key = malloc(key_size+1);
    if(*key == NULL)
      kp_die("Malloc failed in printing key\n");

    // use the index as a suffix for keys (padded by zeros)
    snprintf((char *) *key, key_size+1, "%0*ju", key_size, real_index);
    int len = strlen((char*) *key);
    assert(len == key_size);
  }
}

/* Either fetch the value from the stored ones, or just return
   the same key as normal */
void fetch_value_from_index(int index, void** value){
  if(use_random_values)
    *value = (void*) temp_values[index];
  else {
    *value = (void*) temp_value;
    int len = strlen((char*) *value);

    // Sanity check the length
    if(len != value_size)
      printf("value has length %d, should have %d \n", len, value_size);
    assert(len == value_size);
  }
}

/* Allocates a struct for storing thread-local random integers.
 * srandom() must have been called before calling this function!!!
 *
 * Returns: 0 on success, -1 on error. On success, *ints is set
 * to point to the newly allocated random_ints struct. */
int random_ints_create(random_ints **ints, unsigned int count){
  unsigned int i;
  if (use_rand){
    if(K_DEBUG)
      printf("bypassing random int creation, using rand instead\n");
    return 0;
  }

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
  (*ints)->array = malloc(sizeof(long int) * count);
  if (! (*ints)->array) {
    kp_error("malloc((*ints)->array) failed\n");
    free(*ints);
    *ints = NULL;
    return -1;
  }

  /* Use random() instead of rand() - both are pseudo-random, but random()
   * is alleged to have better quality. */
  for (i = 0; i < count; i++) {
    ((*ints)->array)[i] = random();
  }
  (*ints)->count = count;
  (*ints)->idx = 0;

  return 0;
}

/* Don't pass NULL to this function, or you will pay dearly!! */
long int random_ints_next(random_ints *ints){
  int i;
  if (use_rand){
    i = random();
    return i;
  }
  if (!ints)
    kp_die("ints is NULL!\n");

  i = ints->array[ints->idx];
  ints->idx += 1;
  if (ints->idx == ints->count) {
    ///    kp_warn("cycling around random_ints array!\n");
    ints->idx = 0;
  }

  return i;
}

/* Frees a random_ints struct. */
void random_ints_destroy(random_ints *ints){
  if (use_rand){
    if(K_DEBUG)
      printf("bypassing random int destroy\n");
    return;
  }

  if (ints) {
    if (ints->array) {
      free(ints->array);
      ints->array = NULL;
    }
    free(ints);
    ints = NULL;
  }
}


/* Performs a random operation on a single key
 * Used to slam the local version table... lots of ops on one vt
 * Returns: -1 if there was an error, 0 if the random request was a
 * put-insert, 1 if the random request was a put-append, or 2 if the
 * random request was a get.
*/
int random_on_single_key(void *store, void *trans, int i, char* key,
    random_ints *ints){
  int retval, rc = 0;
  void *value = NULL;
  size_t size;

  if( key == NULL || store == NULL){
    kp_error("Null parameter to random_on_single_key\n" );
    return RET_ERROR;
  }
  
  
  /* Pick put or get */
  rc = biased_put_get_coin(ints);
  if(rc  == 0){    
    if(check_workload)
      wp_numupdate++; //assume that it's an update (only wrong once)
    fetch_value_from_index(i, &value);
    
    if(K_DEBUG)
      printf("DEBUG: Request is a put (key=%s, value=%s)\n",
             (char *)key, (char *)value);

    rc = generic_put(store, trans, key, value, value_size+1);
    if (rc == 0)
      retval = RET_UPD_OKAY;  //put-append
    else
      retval = RET_UPD_ERROR; //error code
  }
  if(rc ==2){
    if(check_workload)
      wp_numdel++;
    if(K_DEBUG)
      printf("DEBUG: Request is a delete (key=%s) \n", key);
    rc = generic_delete(store, trans, key);
    if (rc == 0)
      retval = RET_DEL_OKAY; //delete
    else
      retval = RET_DEL_ERROR; //error code
  }
  else{
    if(K_DEBUG)
      printf("DEBUG: Request is a get (key=%s) \n", key);
    if(check_workload)
      wp_numget++;

    rc = generic_get(store, key, (char **)&value, &size);
    if (rc == 0)
      retval = RET_GET_OKAY;  //get
    else
      retval = RET_GET_ERROR; //error-code
    
    if(free_gotten_vals){
      free(value);
      value = NULL;
    }
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
                               int partition_limit, random_ints *ints){
  int retval, rc = 0;
  void *value = NULL;
  //  size_t size;

  int key_num = -1;
  char *key = NULL;
  
  if( store == NULL){
    kp_error("Null parameter to random_on_partitioned_key\n" );
    return RET_ERROR;
  }

  /* Don't put if we have no more slots left in this partition
     Automatically put if there's no keys put in our partition or
     if the random flip says to*/
  rc = biased_put_get_coin(ints);
  if((*partition_count < partition_limit) &&
     ((*partition_count == 0) || (rc == 0))){

    /* Automatically put to a new key if there's none to update */
    rc = biased_update_coin(ints);
    if((*partition_count == 0) | (rc == 0)){
      if(check_workload)
        wp_numinsert++;

      /* Make a new key. Increment partition_count  which is the index
       * into the temp_keys array for this partition.
       * Don't have to worry about the atomicity since this is just
       * into a partition */
      key_num = *partition_count + offset;
      (*partition_count)++;
      fetch_key_from_index(key_num, (void **)&key);
      fetch_value_from_index(key_num, &value);

      if(K_DEBUG)
        printf("DEBUG: Request is put to a new key (key=%s, value=%s)\n",
               (char*) key, (char*) value);

      rc = generic_put(store, trans, key, value, value_size+1);
      free(key);
      if (rc == 0)
        retval = RET_PUT_OKAY;  //put-insert
      else
        retval = RET_PUT_ERROR; //error code
    }
    else{
      if(check_workload)
        wp_numupdate++;
      key_num = pick_random_key(*partition_count, ints) + offset;
      fetch_key_from_index(key_num, (void **) &key);
      fetch_value_from_index(key_num, &value);

      if(K_DEBUG)
        printf("DEBUG: Request is put to an old key (key=%s, value=%s)\n", 
               (char*) key,(char*) value);

      rc = generic_put(store, trans, key, value, value_size+1);
      free(key);
      if (rc == 0)
        retval = RET_UPD_OKAY;  //put-append
      else
        retval = RET_UPD_ERROR; //error code

    }
  }
  else if(rc == 2){
    if(check_workload)
      wp_numdel++;
    key_num = pick_random_key(*partition_count, ints) + offset;
    fetch_key_from_index(key_num, (void **) &key);
    if(K_DEBUG)
      printf("DEBUG: Request is a delete (key=%s) \n", key);
    rc = generic_delete(store, trans, key);
    free(key);
    if (rc == 0)
      retval = RET_DEL_OKAY; //delete
    else
      retval = RET_DEL_ERROR; //error code
  }
  else{
    /*    
    if(check_workload)
      wp_numget++;
    key_num = pick_random_key(*partition_count, ints) + offset;
    fetch_key_from_index(key_num, (void **) &key);

    if( key == NULL ){
      kp_die("got a NULL key for key_num %d; something is wrong\n",key_num);
      return RET_GET_ERROR;
    }

    if(K_DEBUG)
      printf("DEBUG: Request is a get (key=%s) \n", key);

    rc = generic_get(store, key, (char **)&value, &size);
    free(key)
    if (rc == 0)
      retval = RET_GET_OKAY;  //get
    else
      retval = RET_GET_ERROR; //error-code

    if(free_gotten_vals){
      free(value);
      value = NULL;
      }*/
    //always go to the prepopulated numbers
    retval = create_random_get(store, trans, i, ints);
  }

  return retval;
}

int create_random_request(void *store, void *trans, int i, random_ints *ints){
  int retval, rc = 0;
  void *value = NULL;

  int key_num = -1;
  char *key = NULL;

  if( store == NULL){
    kp_error("Null parameter to create_random_request\n" );
    return RET_ERROR;
  }  

  /* Pick put or get */
  /* Don't put if we have no more slots left in this store
     Automatically put if there's no keys put in our store or
     if the random flip says to*/
  rc = biased_put_get_coin(ints);
  if(((num_keys == 0) || (rc == 0))){

    /* Automatically put to a new key if there's none to update */
    rc = biased_update_coin(ints);
    if((num_keys == 0) | (rc == 0)){
      if(check_workload)
        wp_numinsert++;
      /* Make a new key. Increment num_keys, which is the maximum index
       * into the temp_keys array that other operations can choose. The
       * copy-and-increment of num_keys would ideally be atomic, but it
       * two threads interleave here, the worst that can happen is that
       * both threads get the same key_num, and hence one of the put-
       * inserts will actually be a put-append, but that's not terrible */
      key_num = num_keys;
      num_keys++;
      fetch_key_from_index(key_num, (void **) &key);
      fetch_value_from_index(key_num, &value);

      if(K_DEBUG)
        printf("DEBUG: Request is put to a new key (key=%s, value=%s)\n",
               (char*) key, (char*) value);

      rc = generic_put(store, trans, key, value, value_size+1);
      free(key);
      if (rc == 0)
        retval = RET_PUT_OKAY;  //put-insert
      else
        retval = RET_PUT_ERROR; //error code
    }
    else{
      if(check_workload)
        wp_numupdate++;
      key_num = pick_random_key(num_keys, ints);
      fetch_key_from_index(key_num, (void **) &key);
      fetch_value_from_index(key_num, &value);

      if(K_DEBUG)
        printf("DEBUG: Request is put to an old key (key=%s, value=%s)\n", 
               (char*) key,(char*) value);

      rc = generic_put(store, trans, key, value, value_size+1);
      free(key);
      if (rc == 0)
        retval = RET_UPD_OKAY;  //put-append
      else
        retval = RET_UPD_ERROR; //error code
    }
  }
  else if(rc == 2){
    if(check_workload)
      wp_numdel++;
    key_num = pick_random_key(num_keys, ints);
    fetch_key_from_index(key_num, (void **) &key);
    if(K_DEBUG)
      printf("DEBUG: Request is a delete (key=%s) \n", key);
    rc = generic_delete(store, trans, key);
    free(key);
    if (rc == 0)
      retval = RET_DEL_OKAY; //delete
    else
      retval = RET_DEL_ERROR; //error code
  }
  else{
    retval = create_random_get(store, trans, i, ints);
  }
  return retval;
}

int create_random_get(void *store, void *trans, int i, random_ints *ints){
  int retval, rc = 0;
  void *value = NULL;
  size_t size;

  int key_num = -1;
  char *key = NULL;

  if( store == NULL ){
    printf("Null parameter in create_random_get\n");
    return RET_GET_ERROR;
  }

  /* Randomly choose a key from the range of keys that we've already
   * put to: */
  if(check_workload)
    wp_numget++;
  key_num = pick_random_key(PREPOP_NUM, ints);
  eval_debug("WORKER: picked random key=%d (of %d prefilled) for get; "
             "merge_every=%d\n", key_num, PREPOP_NUM, merge_every);
  fetch_key_from_index(key_num, (void **)&key);

  if( key == NULL ){
    kp_die("got a NULL key for key_num %d; something is wrong\n",key_num);
    return RET_GET_ERROR;
  }

  if(K_DEBUG)
    printf("DEBUG: Request is a get (key=%s) \n", key);

  rc = generic_get(store, key, (char **)&value, &size);
  free(key);
  if (rc == 0)
    retval = RET_GET_OKAY;  //get
  else
    retval = RET_GET_ERROR; // error_code

  if(free_gotten_vals){
    free(value);
    value= NULL;
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
  int del_error_count = 0;
  int put_error_count = 0;
  int upd_error_count = 0;
  int get_error_count = 0;
  void * trans = NULL;

  /* Start random workload, get cost of average call */
  struct timeval *start = malloc(sizeof(struct timeval));
  if (!start)
    kp_die("malloc(start) failed\n");
  struct timeval *end = malloc(sizeof(struct timeval));
  if (!end)
    kp_die("malloc(end) failed\n");

  printf("***NOW DOING SINGLE THREADED TEST ON RANDOM WORKLOAD***\n");

  /* create a random workload */
  bool trans_merged=true;
  int num_puts = 0;
  int num_dels = 0;
  int ret = 0, conflicts = 0, commits = 0;

  /* Start counters and timers */
  reset_thpt();
  gettimeofday(start, NULL);
  start_timing(&timers);

  #ifdef PIN_INSTALLED
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(1);
  #endif

  /* Do the actual run */
  trans = generic_trans_start(store);
  for (i=0; i < num_iterations; i++){
    rc = create_random_request(store, trans,  i, ints);
    if (rc == RET_ERROR) {
      error_count++;
      continue;
    }else if (rc == RET_DEL_OKAY || rc ==RET_DEL_ERROR) { //insert,delete or update
      num_dels++;
      if( rc == RET_DEL_ERROR)
        del_error_count++;
    } else if (rc == RET_PUT_OKAY || rc == RET_UPD_OKAY ||
               rc == RET_PUT_ERROR || rc == RET_UPD_ERROR) { 
      num_puts++; //okay to count dels as puts here
      trans_merged = false;
      if( rc == RET_PUT_ERROR)
        put_error_count++;
      if( rc == RET_UPD_ERROR)
        upd_error_count++;
    }
    if( rc == RET_GET_ERROR)
      get_error_count++;

    /* Merge every once in a while: only track puts, ignore the gets! */
    if (((num_puts+num_dels) %merge_every) == 0 
        && !trans_merged) {  //don't merge on first put
      if(K_DEBUG)
        printf("merging at %u puts (%d loops)\n", num_puts, i);
      ret = generic_trans_end(store, trans);
      if (ret)
        conflicts++;
      commits++;
      trans_merged=true;
      trans = generic_trans_start(store); // start a new one
    }
  }
  if(!trans_merged) {
    ret = generic_trans_end(store, trans);
    if (ret)
      conflicts++;
    commits++;
  }
  #ifdef PIN_INSTALLED
  INSTRUMENT_OFF();
  #endif

  stop_timing(&timers);
  gettimeofday(end, NULL);

  /* Get timing information */
  int usec,  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  int time_usec = 1000000*sec+usec;
  float cost_workload = (float)time_usec/(float)num_iterations;

  /* Report reuslts, including throughput info */
  printf("For random_workload: \n\t time elapsed %d:%d (sec:usec)"
         "= %d usecs \n",
         sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n", 
	 num_iterations, cost_workload);
  printf("\t Puts: %d, Dels: %d, Gets: %d\n",
         num_puts, num_dels, num_iterations - num_puts - num_dels);
  printf("\t Put errors: %d, Del errors: %d, Get errors: %d\n",
         put_error_count+upd_error_count, del_error_count, get_error_count);
  printf("\t Errors: %d Conflicts: %d (of %d commits)\n\n", 
         error_count, conflicts, commits);
  print_timing("RANDOM-WORKLOAD", &timers, num_iterations); 

  free(start);
  start = NULL;
  free(end);
  end = NULL;
  return rc;
}

#define INSERTME 1
#define UPDATEME 2
#define GETME 3
#define DELETEME 4
#define CASSNOP 5
int individual_function_eval(int iteration_count, int factor, void *store,
                             void **trans, int op, char* opstring,
                             int clear_count_num, int mergenum,
                             bool randomized){
  int actual_count, key_num = -1;
  int i, j, rc = 0;
  void *value = NULL;
  size_t size;
  char *key = NULL;
  int ret = 0, conflicts = 0, commits = 0;;
  float cost;
  int usec, sec;
  int time_usec;
  kp_stats stats_before, stats_after;  //just allocate on stack

  /* Setup Timers */
  struct timeval *start = malloc(sizeof(struct timeval));
  if (!start)
    kp_die("malloc(start) failed\n");
  struct timeval *end = malloc(sizeof(struct timeval));
  if (!end)
    kp_die("malloc(end) failed\n");
  kp_timers timers;

  /* (re)set counters */
  rc = 0;
  conflicts = 0;
  commits = 0;
  actual_count = iteration_count*factor;
  
  if(mergenum == 0){ // don't merge
    mergenum = (factor*iteration_count)+1;
  }


  /* start timers */
  gettimeofday(start, NULL);
  start_timing(&timers);
  kp_stats_get(&stats_before);

  #ifdef PIN_INSTALLED
  INSTRUMENT_ON();
  CLEAR_COUNTS_SET_ID(clear_count_num);
  #endif

  reset_thpt();
  if(op != GETME)
    *trans = generic_trans_start(store);

  for( i = 0; i < factor; i++){
    for (j = 0; j < iteration_count; j++){

      /* Get new key for inserts */
      if (op == INSERTME){
        key_num = num_keys;
        num_keys++;
      }
      else if (op == UPDATEME || op == GETME || op == DELETEME){
        if(!randomized)
          key_num = j;
        else
          key_num = pick_random_key(num_keys, NULL);
      }

      fetch_key_from_index(key_num, (void **)&key);


      /* Get new value for puts */
      if( op == UPDATEME || op == INSERTME){
        fetch_value_from_index(key_num, &value);
        int len  = strlen(temp_value);
        assert(len == value_size);
        rc = generic_put(store, *trans, key, value, value_size+1);
        len  = strlen(temp_value);
        assert(len == value_size);
      }
      if (op == DELETEME){
        rc = generic_delete(store, *trans, key);
      }
      if( op == GETME){
        if(free_gotten_vals){
          rc = generic_get(store, key, 
                           (char**)&dest_value, 
                           (size_t*)&size);
          if (! dest_value){
            free(dest_value);
            dest_value = NULL;
          }
        }
        else {
          rc = generic_get(store, key, 
                           (char**)&dest_values[i*iteration_count+j], 
                           (size_t*)&size);
        }
      }
      #ifdef CASSANDRA_INSTALLED
      if( op == CASSNOP){
        rc = cassandra_noop1((cassandra_node *)store);
      }
      #endif
      if(rc != 0){
        actual_count--;
      }   
      free(key);
      if( op != GETME){
        /* Merge every once in a while: */
        if ((j % mergenum) == 0 && 
            (j > 0 || mergenum ==1)) {  //don't merge on first loop
          if(K_DEBUG)
            printf("merging at %d\n", j);
          ret = generic_trans_end(store, *trans);
          if (ret)
            conflicts++;
          commits++;
          if((i != factor-1) || (j != iteration_count-1))
            *trans = generic_trans_start(store);
        }
      }
    }
    if( (mergenum != (factor*iteration_count)) && //don't merge on mergnum=0
        ((j % mergenum) != 0) && (mergenum != 1) && //we didn't already merge
        (op != GETME)){
      ret = generic_trans_end(store, *trans);
      if (ret)
        conflicts++;
      commits++;
      if((i != factor-1) || (j != iteration_count-1))
        *trans = generic_trans_start(store);
    }
  }
  #ifdef PIN_INSTALLED
  INSTRUMENT_OFF();
  #endif

  kp_stats_get(&stats_after);   //right spot??
  stop_timing(&timers);
  gettimeofday(end, NULL);
  
  /* Figure out the timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost = (float)time_usec/(float)actual_count;

  /* Print results */
  printf("For %s: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
         opstring, sec, usec, time_usec);
  printf("\t iterations %d (merge every %d -> %d merges)\n"
      "\t average cost %f usecs\n", 
      actual_count, mergenum, actual_count/mergenum, cost);
  printf("\t Conflicts: %d (of %d commits)\n\n", conflicts, commits);
  print_timing(opstring, &timers, actual_count);
  kp_stats_subtract(&stats_before, &stats_after, &stats_after);
  if (print_stats)
    kp_stats_print(&stats_after, opstring);

  /* We're not timing right now, so should be ok to do this freeing here. */
  if( op == GETME && !free_gotten_vals){
    for( i = 0; i < iteration_count*factor; i++){
      free(dest_values[i]);
      dest_values[i] = NULL;
    }
  }
  free(start);
  start = NULL;
  free(end);
  end = NULL;

  return rc;
}



/* Tests the latency of each operation currently in our arsenal
 * put-insert, put-append, get, delete, garbage collect.
 * Reports the latency as timing information. 
 * Only collects info on successful calls. 0 success, -1 failure
 */
int latency_evaluation(int num_iterations, void* store){
  int rc = 0;
  void *trans = NULL;
  printf("***NOW DOING LATENCY TEST ON INDIVIDUAL FUNCTIONS ***\n");


  /* Get the average cost of put-insert */
  /*=================================================================*/
  rc = individual_function_eval(num_iterations, 1, store, &trans,
                                INSERTME, "put-insert", 2, merge_every, 
                                false);
  
  /* Get the average cost of put-append */
  /*=================================================================*/
  rc = individual_function_eval(num_iterations, MULT_FACTOR, store, &trans,
                                UPDATEME, "put-append", 3, merge_every,
                                false);

  /* Get the average cost of get-current */
  /*=================================================================*/
  rc = individual_function_eval(num_iterations, MULT_FACTOR, store, &trans,
                                GETME, "get", 4, merge_every, false);

  /* Get the average cost of delete */
  /*=================================================================*/
  rc = individual_function_eval(num_iterations, MULT_FACTOR, store, &trans,
                                DELETEME, "delete", 4, merge_every, false);

  return rc;
}


/* Calls the key-value store internal measurement function
 * to evaluate the size and proportions of the store
 * Works up some random workload first. 
 */
int local_keyvalue_size(int num_iterations, kp_kv_local *kv) {
  kp_error("print stats not implemented\n");
  return 0;
}



/* For the thread-test evaluations, keep each not-main thread
 * busy with a random workload constantly, to maximize numbers
 */
void *worker_thread_entrypoint(void *arg){
  int i, rc = 0;
  int num_puts;
  int num_dels;
  int total_ops;
  char *ret_string;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  void *master;
  void *worker;
  void *trans = NULL;
  int error_count = 0;
  int put_error_count = 0;
  int del_error_count = 0;
  int upd_error_count = 0;
  int get_error_count = 0;
  int ret = 0, conflicts = 0, commits = 0;
  random_ints *ints;
  /* setup thread, counters, and store */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;
  worker = generic_worker_create(master, 0);
  num_dels = 0;
  num_puts = 0;
  total_ops = 0;
  i = 0;
  ints = thread_args->ints;
  
  /* setup timers */
  struct timeval *start = malloc(sizeof(struct timeval));
  if (! start)
    kp_die("malloc(start) failed\n");
  struct timeval *end = malloc(sizeof(struct timeval));
  if (! end)
    kp_die("mallocs(end) failed\n");

  /* set up partition if necessary (use split workload) */
  int key_count = 0;
  int partition_limit = max_kv_size/(thread_args->num_threads);
  int offset = partition_limit*(thread_args->my_id);

  int recent_puts = 0;
  /* Do work as long as measurement is still going on */
  trans = generic_trans_start(worker);
  while(measurement_in_progress){
    if(total_ops == 0) { // first operation!
      if(K_DEBUG)
        kp_print("actually starting now\n");
      gettimeofday(start,NULL);
    }
    if(use_get_workload){
      rc = create_random_get(worker, trans, i, ints);
    }
    else if (use_split_workload){
      rc =  random_on_partitioned_keys(worker, trans, i,
                                 offset, &key_count,
                                 partition_limit, ints);                                  
    }
    else{
      rc = create_random_request(worker, trans, i, ints);
    }
    if (rc == RET_ERROR) {
      error_count++;
      continue;
    } else if (rc == RET_DEL_OKAY || rc == RET_DEL_ERROR){
      num_dels++;
      recent_puts++;
      if( rc == RET_DEL_ERROR)
        del_error_count++;
    }else if(rc == RET_PUT_OKAY || rc == RET_UPD_OKAY ||
             rc == RET_PUT_ERROR || rc == RET_UPD_ERROR) { 
      num_puts++;
      recent_puts++;
      if( rc == RET_PUT_ERROR)
        put_error_count++;
      if( rc == RET_UPD_ERROR)
        upd_error_count++;
    }      
    if( rc == RET_GET_ERROR)
      get_error_count++;


    total_ops++;i++;

    // Merge every once in a while:
    if(recent_puts == merge_every){
      if(K_DEBUG)
        printf("merging at %u puts, %u dels  (%d loop) \n", 
               num_puts, num_dels, i);
      ret = generic_trans_end(worker, trans);
      if (ret)
        conflicts++;
      commits++;
      eval_debug("WORKER: just committed (%d)\n", commits);
      trans = generic_trans_start(worker);
      recent_puts = 0;
    }
  }
  if(recent_puts != merge_every && recent_puts != 0){
    ret = generic_trans_end(worker, trans);
    if (ret)
      conflicts++;
    commits++;
    eval_debug("WORKER: just committed (%d)\n", commits);
  }

  gettimeofday(end,NULL); //Get end time AFTER the last merge
  int usec, sec, time_usec;
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec--;
    usec+=1000000;
  }
  time_usec = 100000*sec+usec;
  
  /* store results  in global stuff
   * TODO lock on this!
   */
  thpt_numputs = thpt_numputs + num_puts;
  thpt_numdels = thpt_numdels + num_dels;
  thpt_numops = thpt_numops + total_ops;
  thpt_numgets = thpt_numgets+ (total_ops - num_puts- num_dels);
  total_conflicts = total_conflicts + conflicts;
  total_commits = total_commits + commits;
  total_error_count = total_error_count + error_count;
  total_put_error_count = total_error_count + put_error_count;
  total_upd_error_count = total_error_count + upd_error_count;
  total_del_error_count = total_error_count + del_error_count;
  total_get_error_count = total_error_count + get_error_count;

  free(start);
  free(end);

  /* Destroy (or fake-destroy) a worker */
  generic_worker_destroy(worker);

  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
  if (!ret_string)
    kp_die("malloc(ret_string) failed\n");
  snprintf(ret_string, RET_STRING_LEN, "success_%lu", tid);
  if(K_DEBUG)
    printf("thread %lu returning string=%s\n", tid, ret_string);
  return (void *) ret_string;
}

void ramp_up_threads(int num_threads, char* bench_string){
  int rc, i = 0;
  void *ret_thread;
  int cpu;
  pthread_attr_t attr;
  pthread_t threads[MAX_THREADS];
  benchmark_thread_args thread_args[MAX_THREADS];
  kp_stats stats_before, stats_after;  //just allocate on stack

  /* Check args */
  if(num_threads <= 0){
    kp_die("invalid num_threads=%d\n", num_threads);
  }
  kp_debug("pid=%d, num_threads=%d\n", getpid(), num_threads);


  /* Create the master and one local worker*/
  void * master = generic_store_create(num_threads, 0);

  /* Pre-allocate random numbers for each thread, _before_ starting any
   * of them: */
  for (i = 0; i < num_threads; i++) {
    rc = random_ints_create(&(thread_args[i].ints), random_int_count);
    if (rc != 0) {
      kp_error("random_ints_create() failed\n");
      return;
    }
  }

  /* Pre populate the store before starting any thread */
  void *worker = generic_worker_create(master, PREPOP_NUM+1);
  int key_num = 0;
  void *value = NULL;
  char *key = NULL;
  void *trans = NULL;
  num_keys = 0;
  trans = generic_trans_start(worker);
  for (i = 0; i < PREPOP_NUM; i++){
    key_num = i;
    num_keys++;
    fetch_key_from_index(key_num, (void **) &key);
    fetch_value_from_index(key_num, &value);
    rc = generic_put(worker, trans, key, value, value_size+1);
    free(key);
    //should probably check numbers, but whatever, we just want to warm it up
  }
  rc = generic_trans_end(worker, trans);
  generic_worker_destroy(worker);
  measurement_in_progress = true;
  

  /* Create threads */
  for (i=0; i < num_threads; i++){
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
    kp_print("pinned new thread to CPU=0x%X\n", cpu);

    /* Set the master and spawn */
    thread_args[i].master = master;
    thread_args[i].num_threads = num_threads;
    thread_args[i].my_id = i;
    
    rc = pthread_create(&threads[i], &attr, &worker_thread_entrypoint, 
                          (void *)(&(thread_args[i])));

    if(rc != 0) {
      if (rc == 22) {
        kp_error("pthread_create() returned error=%d: check that NUM_CPUS "
            "(%d) is not greater than number of physical cores!\n", rc, NUM_CPUS);
      }
      perror("pthread_create failed");
      kp_die("pthread_create() returned error=%d\n", rc);
    }
    

    /* Destroy the attributes */
    rc = pthread_attr_destroy(&attr);
    if(rc != 0)
      kp_die("pthread_attr_destroy() returned error=%d\n", rc);
  }

  /*=============== MASTER ACTIONS ==================== */
  /* wait */
  kp_stats_get(&stats_before);
  reset_thpt();
  kp_print("sleeping to allow threads enough time\n");
  rc = sleep((unsigned int)KPMT_SLEEP);
  kp_print("waking up from sleep (%d)\n", rc);

  /* Send cancellation requests */
  measurement_in_progress = false;

  kp_stats_get(&stats_after);   //right spot??

  /* Get that worker back! */
  for( i = 0; i < num_threads; i++){
    rc = pthread_join(threads[i], &ret_thread); //wait for them to rec'v quit
    //    if(rc != 0)
    //      kp_die("pthread_join() returned error=%d\n", rc);
    if(ret_thread)
      free(ret_thread);
  }
 
  //kp_stats_get(&stats_after);   //right spot??

  int total_usecs = KPMT_SLEEP*1000000;
  /* Thpt counts are set by exiting threads */
  printf("For multithreaded run %s\n"
         "\t %d total ops (%d puts + %d gets + %d dels)\n"
         "\t %d in runtime (usecs) = %d secs = %f ops/usec\n" 
         "\t %f op/sec put throughput (all threads)\n" 
         "\t %f op/sec get throughput (all threads)\n"
         "\t %f op/sec del throughput (all threads)\n"
         "\t %f op/sec total throughput (all threads)\n"
         "\t Conflicts in worker thread: %d (of %d commits)\n"
         "\t Errors in worker thread: %d\n", bench_string,
         thpt_numops, thpt_numputs, thpt_numgets, thpt_numdels,
         total_usecs, total_usecs/1000000,
         (float)thpt_numops/(float)total_usecs,
         1000000*(float)thpt_numputs/(float)total_usecs,
         1000000*(float)thpt_numgets/(float)total_usecs,
         1000000*(float)thpt_numdels/(float)total_usecs,
         1000000*((float)thpt_numops/(float)total_usecs),
         total_conflicts, total_commits,total_error_count);
  printf("\t Put errors: %d, Del errors: %d, Get errors: %d\n",
         total_put_error_count+total_upd_error_count, 
         total_del_error_count, 
         total_get_error_count);
  kp_stats_subtract(&stats_before, &stats_after, &stats_after);
  if (print_stats)
    kp_stats_print(&stats_after, bench_string);
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
  int num_puts = 0;
  int num_dels = 0;
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
  worker = generic_worker_create(master, 0);
  ints = thread_args->ints;

  int key_count = 0;
  int partition_limit = max_kv_size/(thread_args->num_threads);
  int offset = partition_limit*(thread_args->my_id);
  int error_count = 0;
  int put_error_count = 0;
  int get_error_count = 0;
  int upd_error_count = 0;
  int del_error_count = 0;
  int recent_puts = 0;
  if (!bench_cond || !bench_mutex) {
    kp_die("got a null argument: bench_cond=%p, bench_mutex=%p\n",
        bench_cond, bench_mutex);
  }

  /* Use the lock to start things off */
  rc = pthread_mutex_lock(bench_mutex);
  if (rc != 0) {
    kp_die("pthread_mutex_lock() returned error %d\n", rc);
  }

  trans = generic_trans_start(worker); //This doesn't need to be locked...
  recent_puts = 0;
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
    if(thread_args->slam_local){
      char *key;
      fetch_key_from_index(0, (void **)&key);
      rc = random_on_single_key(worker, trans, i, key, ints);
      free(key);
    }
    else if(use_split_workload){
      rc = random_on_partitioned_keys(worker, trans, i, 
                                      offset, &key_count,
                                      partition_limit, ints);
    }
    else{
      rc = create_random_request(worker, trans, i, ints);
    }
    if (rc == RET_ERROR) {
      error_count++;
      continue;
    }
    else if (rc == RET_DEL_OKAY || rc == RET_DEL_ERROR){
      num_dels++;
      recent_puts++;
      if(rc == RET_DEL_ERROR)
        del_error_count++;
    }
    else if (rc == RET_PUT_OKAY || rc == RET_UPD_OKAY ||
             rc == RET_PUT_ERROR || rc == RET_UPD_ERROR){
      num_puts++;
      recent_puts++;
      if( rc == RET_PUT_ERROR)
        put_error_count++;
      if( rc == RET_UPD_ERROR)
        upd_error_count++;
    }      
    if( rc == RET_GET_ERROR)
      get_error_count++;

    my_completed_ops++;

    /* Merge every once in a while: */
    if(recent_puts == merge_every && recent_puts != 0){
      ret = generic_trans_end(worker, trans);
      if (ret)
        conflicts++;
      commits++;
      trans = generic_trans_start(worker);
      recent_puts = 0;
    }
  }

  /* Finish up by merging in things*/
  if(recent_puts != merge_every && recent_puts != 0){
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

  thpt_numputs = thpt_numputs +  num_puts;
  thpt_numdels = thpt_numdels + num_dels;
  thpt_numgets = thpt_numgets + (my_completed_ops - num_puts - num_dels);
  thpt_numops = thpt_numops + my_completed_ops;
  total_conflicts = total_conflicts + conflicts;
  total_commits = total_commits + commits;
  total_error_count = total_error_count + error_count;
  total_put_error_count = total_error_count + put_error_count;
  total_upd_error_count = total_error_count + upd_error_count;
  total_del_error_count = total_error_count + del_error_count;
  total_get_error_count = total_error_count + get_error_count;

  //  printf("\t Conflicts in worker thread: %d (of %d commits)\n",
  //      conflicts, commits);
  //  printf("\t Errors in worker thread: %d (of %d ops)\n",
  //      error_count, my_completed_ops);
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
                             bool slam_local){
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
  void * master = generic_store_create(num_threads, 0);
 
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

  /* Pre populate the store before starting any thread */
  void *worker = generic_worker_create(master, PREPOP_NUM+1);
  int key_num = 0;
  void *value = NULL;
  char *key = NULL;
  void *trans = NULL;
  num_keys = 0;
  trans = generic_trans_start(worker);
  for (i = 0; i < PREPOP_NUM; i++){
    key_num = i;
    num_keys++;
    fetch_key_from_index(key_num, (void **) &key);
    fetch_value_from_index(key_num, &value);
    rc = generic_put(worker, trans, key, value, value_size+1);
    free(key);
  }
  rc = generic_trans_end(worker, trans);
  generic_worker_destroy(worker);

 /* Create a single thread */
  for (i=0; i < num_threads; i++){
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
    thread_args[i].master = master;
    thread_args[i].num_threads = num_threads;
    thread_args[i].starting_ops = BENCH_EXTEND*iterations;
    thread_args[i].bench_mutex = bench_mutex;
    thread_args[i].bench_cond = bench_cond;

    rc = pthread_create(&threads[i], &attr, &fixed_benchmark_entrypoint, 
                        (void *)(&(thread_args[i])));
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
  int total_usecs = time_usec;

  /* Print results */
  printf("For Fixed %s Benchmark (random+latency):"
         "\n\t benchmark cost %d:%d (sec:usec)"
         "= %d usecs \n",
         bench_string, sec, usec, time_usec);
  printf("\tAt a r-w-u proportion of %.2f-%.2f-%.2f...\n",
         (1-put_probability)*100,
         (put_probability * (1-update_probability))*100,
         (put_probability * update_probability)*100);
  printf("\t %d total ops (%d puts + %d gets + %d dels)\n"
         "\t %d in runtime (usecs) = %d secs = %f ops/usec\n" 
         "\t %f op/sec put throughput (all threads)\n" 
         "\t %f op/sec get throughput (all threads)\n"
         "\t %f op/sec del throughput (all threads)\n"
         "\t %f op/sec total throughput (all threads)\n"
         "\t Conflicts in worker thread: %d (of %d commits)\n"
         "\t Errors in worker thread: %d\n", 
         thpt_numops, thpt_numputs, thpt_numgets, thpt_numdels,
         total_usecs, total_usecs/1000000,
         (float)thpt_numops/(float)total_usecs,
         1000000*(float)thpt_numputs/(float)total_usecs,
         1000000*(float)thpt_numgets/(float)total_usecs,
         1000000*(float)thpt_numdels/(float)total_usecs,
         1000000*((float)thpt_numops/(float)total_usecs),
         total_conflicts, total_commits,total_error_count);
  printf("\t Put errors: %d, Del errors: %d, Get errors: %d\n",
         total_put_error_count+total_upd_error_count, 
         total_del_error_count, 
         total_get_error_count);
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
    ret_thread = NULL;
  }


  /* Cleanup */
  generic_store_destroy(master);
  for (i = 0; i < num_threads; i++) {
    random_ints_destroy(thread_args[i].ints);
  }

  reset_counts();

  return;
}


void fixed_global_benchmark(int num_threads){
  fixed_generic_benchmark(num_threads, "split_keys", false);
}

void fixed_local_benchmark(int num_threads){
  fixed_generic_benchmark(num_threads, "single_key", true);
}


void fixed_percent_benchmark(int num_threads, 
                             float put_prob, 
                             float up_prob,
                             float del_prob){
  /* set probs */
  float save1 = put_probability;
  float save2 = update_probability;
  float save3 = delete_probability;

  put_probability =put_prob;
  update_probability = up_prob;
  delete_probability = del_prob;

  /* run benchmark */
  char *label = malloc(32*sizeof(char));
  snprintf(label, 32, "fixed r:%.0f w:%.0f u:%.0f d:%.0f",
           (1-put_prob-del_prob)*100, 
           put_prob*(1-up_prob)*100,
           put_prob*up_prob*100,
           del_prob*100);
  fixed_generic_benchmark(num_threads, label, false);
  put_probability = save1;
  update_probability = save2;
  delete_probability = save3;
}

void rampup_percent_benchmark(int num_threads, 
                              float put_prob, 
                              float up_prob,
                              float del_prob){
  /* set probs */
  float save1 = put_probability;
  float save2 = update_probability;
  float save3 = delete_probability;

  put_probability =put_prob;
  update_probability = up_prob;
  delete_probability = del_prob;

  /* run benchmark */
  char *label = malloc(32*sizeof(char));
  snprintf(label, 32, "ramp r:%.0f-w:%.0f-u:%.0f-d:%.0f",
           (1-put_prob-del_prob)*100, 
           put_prob*(1-up_prob)*100,
           put_prob*up_prob*100,
           del_prob*100);
  ramp_up_threads(num_threads, label);
  put_probability = save1;
  update_probability = save2;
  delete_probability = save3;
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
  worker = generic_worker_create(master, 0);
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

void *little_workload_wrapper(void *arg){
  char *ret_string;
  unsigned long tid;
  benchmark_thread_args *thread_args;
  void *master;
  void *worker;
  random_ints *ints;
  int i, rc = 0;
  void *trans = NULL;

  /* setup thread */
  tid = pthread_self();
  thread_args = (benchmark_thread_args *) arg;
  master = thread_args->master;
  ints = thread_args->ints;

  /* Warm up the store, since all gets go to prepopulated */
  /* Create an especially large worker to do this */
  worker = generic_worker_create(master, PREPOP_NUM+1);
  int key_num = 0;
  void *value = NULL;
  char *key = NULL;
  trans = generic_trans_start(worker);
  for (i = 0; i < PREPOP_NUM; i++){
    key_num = i;
    num_keys++;
    fetch_key_from_index(key_num, (void **) &key);
    fetch_value_from_index(key_num, &value);
    rc = generic_put(worker, trans, key, value, value_size+1);
    free(key);
  }
  rc = generic_trans_end(worker, trans);
  generic_worker_destroy(worker);

  /* Do work */
  worker = generic_worker_create(master, 0);
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
  worker = generic_worker_create(master, 0);
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
  master = generic_store_create(1, 0);  //1 = num_threads
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
  ret_thread = NULL;
  generic_store_destroy(master);
  random_ints_destroy(ints);
  /*========================================= */
#if 0
  /* Create the master for workload*/
  master = generic_store_create(1, 0);  //1 = num_threads
  thread_args.master = master;
  rc = random_ints_create(&ints, random_int_count);
  if (rc != 0) {
    kp_error("random_ints_create() failed\n");
    return;
  }
  thread_args.ints = ints;
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
  ret_thread = NULL;
  generic_store_destroy(master);
  random_ints_destroy(ints);
#endif
  /*========================================= */
#if 0
  /* Create the master for keyvalue size*/
  master = generic_store_create(1, 0 );  //1 = num_threads
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
  ret_thread = NULL;
  generic_store_destroy(master);
#endif
  /*========================================= */
  printf("\n*** CONCLUDING SINGLE THREADED TESTS***\n\n");

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
         "[--leveldb-ramdisk] <cpus> <iters> <ksize> <vsize> "
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
                     bool *pause_before_start, int *delay)
{
  int c;
  char *endptr;
  int option_index;
  static int pause_flag = 0;
  static struct option long_options[] =
  {
    /* If third member ("flag") is set, then getopt_long() returns 0
     * and the flag variable is set to the fourth member ("val") if
     * the option is found. The flag variable is left unchanged if the
     * option is not found.
     */
    {"pause", no_argument, &pause_flag, 1},
    {"delay", required_argument, NULL, 'd'},
    {"kpvm-dram", no_argument, &WHICH_KEY_VALUE_STORE, 0},
    {"noop", no_argument, &WHICH_KEY_VALUE_STORE, 4},
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
  if (WHICH_KEY_VALUE_STORE < 0 || WHICH_KEY_VALUE_STORE > 5){
    kp_error("unrecognized (%d) key value store \n", WHICH_KEY_VALUE_STORE);
    usage(argv[0]);
  }
}
int main(int argc, char *argv[]){
  int num_threads = 0;
  int rc = 0;
  int i;
  bool pause_before_start = false;
  int delay = 0;

  srandom(time(NULL));

  parse_arguments(argc, argv, &num_threads, &pause_before_start, &delay);
  kp_debug("after parse_arguments(), num_threads=%d, pause_before_start=%s, "
      "delay=%d, do_conflict_detection=%s\n", num_threads,
      pause_before_start ? "true" : "false", delay,
      do_conflict_detection ? "true" : "false");

  max_key = 1U;
  for(i = 0; i < key_size; i++){
    max_key = 10*max_key;
    if(max_key > UINT_64_ALMOST_MAX){
      i = key_size;
      max_key = UINT_64_ALMOST_MAX;
    }
  }

  /*--------------------------------------------------------------*/
  /* Do global allocations. The maximum size we need for the key value
   * store is the number of puts that we may do to any master
   *  The the maximum of:
   *    keys in single_worker_test
   *        (1+3*MULT_FACTOR)*iterations //latency_eval
   *      + PREPOP+Iterations //workload_eval
   *      + 0 for kv_size
   *    keys in a given fixed_benchmark
   *      PREPOP+(BENCH_EXTEND*iterations)*num_threads
   *    keys in a given ramp_up_threads (rough upperbound given ops/sec)
   *      PREPOP+(KPMT_SLEEP*8*10^6)*num_threads
   *    kyes in a given GC_eval
   *      0, currently
   * The local expected can be one of a few things
   *    1. mergesize (conservatively estimate 2x mergesize)
   *    2. prepop number (all done in one transaction)
   *    3. Amount in any other large trans (eg baselatency)
   *  Rather than guess too large on this,we'll set it to the mergesize
   *   which is the most frequent, and then set it per-worker on others
   */
  int single_size = ((2+3*MULT_FACTOR)*iterations)+PREPOP_NUM;
  int ramp_up_size =PREPOP_NUM+
    (KPMT_SLEEP*8*1000000*put_probability);
  int bench_size =  PREPOP_NUM+(BENCH_EXTEND*iterations); 
  max_kv_size = (ramp_up_size > bench_size)? ramp_up_size: bench_size;
  max_kv_size = (max_kv_size > single_size)? max_kv_size: single_size;
  local_expected_max_keys = merge_every*2;
  kp_print("Using %zu for local hash table size (same as merge frequency), "
      "%u for master hash table size\n", local_expected_max_keys,
      max_kv_size);

  /* With key_size = 32 and val_size = 128, the inner term is 343 bytes.
   *   max_kv_size = 1,000,000 -> 343000000 bytes = 327.11 MB, which
   *     should fit in memory...
   *   max_kv_size = 10,000,000 -> 3430000000 bytes = 3.19 GB, which
   *     may not fit in memory!
   */
  unsigned int total_allocation_size = 0;
  if(! free_gotten_vals){
    total_allocation_size += max_kv_size*sizeof(void*)+
      max_kv_size*(value_size+1);
  }
  if(use_random_values){
    total_allocation_size+= max_kv_size*sizeof(void*)+
      max_kv_size*(value_size+1);
  }
  if(use_random_keys){
    total_allocation_size+= max_kv_size*sizeof(char*)+
      max_kv_size*(key_size+1)*sizeof(char);
  }
  kp_print("Total memory allocation size (just evaluation, not kv stores "
      "themselves) could be up to %u bytes (%u MB)\n",
      total_allocation_size, total_allocation_size / (1<<20));

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
  if(! free_gotten_vals){
    kp_warn("Storing gotten values. Space-expensive\n");
    dest_values = malloc(max_kv_size*sizeof(void*));
    if (!dest_values)
      kp_die("malloc(dest_values) failed\n");
  }

  if(use_random_values){
    kp_warn("Pre-creating values. Space-expensive\n");
    temp_values = malloc(max_kv_size*sizeof(void*));
    if( !temp_values)
      kp_die("malloc(temp_values) failed\n");
    for(i = 0; i < max_kv_size; i++){
      temp_values[i] = malloc(value_size+1);
      if( !temp_values[i])
        kp_die("malloc(temp_values[%d]) failed\n", i);
      rc = create_random_value(value_size, temp_values[i]);
    }
  }
  else{
    temp_value = malloc(value_size+1);
    if(! temp_value)
      kp_die("malloc(temp_value) failed\n");
    rc = create_random_value(value_size, temp_value);
  }
    
  if(use_random_keys){
    kp_warn("Pre-creating keys. Space-expensive\n");
    temp_keys = malloc(max_kv_size*sizeof(char*));
    if( !temp_keys)
      kp_die("malloc(temp_keys) failed\n");
    for(i = 0; i < max_kv_size; i++){
      temp_keys[i] = malloc(key_size+1);
      if( !temp_keys[i])
        kp_die("malloc(temp_keys[%d]) failed\n", i);
      rc = create_random_key(key_size, temp_keys[i]);
    }
  }

  if(!use_rand){
    //This is off, but I'll let it slide for now...
    random_int_count = max_kv_size/num_threads;
    printf("Will create %u random integers for each child thread\n",
           random_int_count);
  }

  printf("Now starting evaluation for key-value store: %s\n",
     WHICH_KEY_VALUE_STORE == 0 ? "kp_kvstore" :
         WHICH_KEY_VALUE_STORE == 5 ? "NOOP": "unknown!");
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
  printf("Starting single-threaded tests\n");

  pid_t c;
  do_forked_run((void)(*single_worker_test));

  if(num_threads >= 1){
    printf("Starting multi-threaded tests: num_threads=%d, NUM_CPUS=%d\n",
           num_threads, NUM_CPUS);
    
    do_forked_run((void)(*rampup_percent_benchmark), num_threads, 0.2, 0.7, 0.01);

    printf("***CONCLUDING THREAD EVALUATION***\n");
  }
  /*--------------------------------------------------------------*/
#ifdef KP_EVAL_LOG
  fclose(log_file);
#endif
  
  /* Free random keys and values */
  if(! free_gotten_vals){
    free(dest_values);
    dest_values = NULL;
  }
  if(use_random_values){
    free(temp_values);
    temp_values = NULL;
  }
  else{
    free(temp_value);
    temp_value = NULL;
  }
  if(use_random_keys){
    free(temp_keys);
    temp_keys = NULL;
  }


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
