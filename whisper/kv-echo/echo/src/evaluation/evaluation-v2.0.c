/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: nil
 * vi: set shiftwidth=2 tabstop=2 expandtab:
 * :indentSize=2:tabSize=2:noTabs=true:
 *
 * Katelin Bailey & Peter Hornyack
 * 8/28/2011
 */

#include "../include/kp_kvstore.h"
#include "kp_macros.h"
#include "../include/kp_kv_local.h"
#include "../include/kp_kv_master.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <sched.h>

/* These two constants define the range of CPUs that the worker threads
 * will be placed on: [CPU_OFFSET, CPU_OFFSET + NUM_CPUS - 1]. If
 * the high end of that range is greater than the number of CPUs on
 * the system, who knows what will happen.
 */
#define NUM_CPUS 2
#define CPU_OFFSET 0
#define MAX_THREADS 256
#define MAX_KV_SIZE 1000000


/* Consistency mode determines
 * where gets go (master or local)
 * how often we merge (wrt puts)
 */
//#define MASTER_CONSISTENCY_MODE MODE_WEAK
#define MASTER_CONSISTENCY_MODE MODE_SNAPSHOT
//#define MASTER_CONSISTENCY_MODE MODE_SEQUENTIAL

#define USE_OTHER_TIMERS 1
#define K_DEBUG 0      //Print Debug messages
#define MULT_FACTOR 3  //For cheap operations, # of extra times to iterate
#define RET_STRING_LEN 64

/* Structure to push arguments to the worker */
typedef struct worker_thread_args_struct {
	cpu_set_t cpu_set;
	kp_kv_master *master;
} worker_thread_args;


/* Parameters that vary the evaluation */
int iterations = 100000;            // Number of iterations to get average
int merge_every = 1000;             // Number of operations before we merge
int key_size = 8;                   // Uniform size for all keys
int value_size = 16;                // Uniform size for all values
float put_probability = .4;          // Probability that next request is a put
float update_probability = .7;       // Prob. that next put is to existing val
float care_about_probability = 1.0;  // Prob. that this is a value we keep
float locality_probability = 0.2;     // Prob. that this request is a hot req.
int   locality_num = 100;

/* Keep track of some keys so we can run random workload */
int num_keys = 0;                    // For an instance, num unique keys in set
char **keys;                         // Keys (stored for updates)
int *vers;                           // How many versions per key (for gets)

/* Do some block allocations at initialization */
void **dest_values;                  // Generate a gazillion potential values 
void **temp_values;                  // Generate a gazillion potential values 
char **temp_keys;                    // Generate a gazillion potential keys 
void **second_values;                // Space for malloc copies to go (vals)
void **second_keys;                  // Space for malloc copies to go (keys)


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



void reset_counts(void){
  num_keys = 0;
}

/* Uses a biased flip to determine if it's a put-new or put-old */
int biased_update_coin(void){
  int r = rand();
  float rd = (float)r/(float)RAND_MAX;
  if(rd > update_probability)
      return 0; // put
  else
    return 1;   // update
}

int biased_hotkey_coin(void){
  int r = rand();
  float rd = (float)r/(float)RAND_MAX;
  if(rd < locality_probability){
    if(K_DEBUG)
      printf("HOT KEY (%f/%f) ...", rd, locality_probability);
    return 0; // hotkey
  }
  else{
    if(K_DEBUG)
      printf("Normal (%f/%f)...", rd, locality_probability);
    return 1;   // normal
  }
}

/* Uses a biased flip to determine if it's a put or a get */
int biased_put_get_coin(void){
  int r = rand();
  float rd = (float)r/(float)RAND_MAX;
  if(rd > put_probability)
      return 1; // gets
  else
    return 0;   // puts
}

/* Pick any random key from those we've added (for gets/deletes/updates) */
int pick_random_key(int input_num_keys){
  int r = rand();
  float rd = (float)r/(float)RAND_MAX ;
  r = rd*(input_num_keys-1);
  if(K_DEBUG)
    printf("(%d/%d)\n", r, input_num_keys);
  return r;
}

/* Pick a random version number from those possible (for gets/deletes) */
int pick_random_version(int max){
  int r = rand();
  float rd = (float)r/(float)RAND_MAX ;
  r = rd*max;
  return r;
}

/* Creates a random n-character null-terminated string for a key */
int create_random_key(int size, char *key){
  int i, r,rc = 0;
  float rd; 
  char c;
  
  sprintf(key, "%c", 0 );
  for (i = 0; i < size; i++){
    r = rand();
    rd = (float)r/(float)RAND_MAX;
    r = rd*26;
    c = r + 'a';
    key[i] = c;
    //    sprintf(key, "%s%c", key, c);
      //pjh: this seems inefficient, why not just index into key[i]?
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




/* Picks a request based on the global parameters listed above,
 * performs that request and returns 0 for success, -1 for fail.
 */
int create_random_request(kp_kv_local *kv, int i){
  int key_num, rc = 0;
  void *value = NULL;
  size_t size;
  char *key = NULL;
  
  /* Pick put or get */
  rc = biased_put_get_coin();
  if((num_keys < MAX_KV_SIZE) && ((num_keys == 0) || (rc == 0))){
    if(K_DEBUG)
      printf("DEBUG: Request is a put...\n");

    /* Decide if we're doing hot keys */
    rc = biased_hotkey_coin();
    if (num_keys <= locality_num)
      rc = 0;
    if (rc == 0){
      /* If doing hot keys, automatically go to update on hot key */
      if(num_keys <= locality_num){
        rc = 0;
        if(K_DEBUG)
          printf("PICKING TO DO A HOT KEY INSERT %d\n", num_keys);
      }
      else{
        key_num = pick_random_key(locality_num);
        rc = 1; // NOT an insert put
        if(K_DEBUG)
          printf("PICKING TO DO A HOT KEY UPDATE %d\n", key_num);
      }
    }
    else{
      /* else, continue onto normal pick/update */
      key_num = pick_random_key(num_keys-locality_num) + locality_num;
      rc = biased_update_coin();
      if(K_DEBUG){
        if (rc == 0)
          printf("PICKING TO DO A NORMAL INSERT %d\n", key_num);
        else
          printf("PICKING TO DO A NORMAL UPDATE %d\n", key_num);
      }
    }

    /* pick update or append */
    if((num_keys == 0) | (rc == 0)){

      if(K_DEBUG)
        printf("DEBUG: \tof a new key...\n");

      /* Make a new key */
      key = temp_keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key); 

      /* pick a value */
      value = temp_values[i];
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* Keep ourselves the key for later */
      key_num = num_keys;
      num_keys++;
      keys[key_num] = key;
      vers[key_num] = 0;

      /* Set its value */
      rc = kp_local_put(kv, key, value, value_size+1);

      if(K_DEBUG && rc == UINT64_MAX)
        printf("DEBUG: Return from put %d\n", rc);
      
      // TODO find out which aren't returning right
      if(rc == UINT64_MAX)
        printf("DEBUG: Return from put %d\n", rc);
    }
    else{
      if(K_DEBUG)
        printf("DEBUG: \tof an old key...\n");

      /* Pick a key */
      key = keys[key_num];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);

      /* pick a value */
      value = temp_values[i];
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* Change the value */
      if( kv == NULL || key == NULL || value == NULL){
        printf("NULL VALUE IN UPDATE KEY %d\n", key_num);
        return -1;
      }
      rc = kp_local_put(kv, key, value, value_size+1);

      // TODO fine out which aren't returning right
      if(rc == UINT64_MAX)
        printf("DEBUG: Return from update %d\n", rc);

      if(K_DEBUG && rc == UINT64_MAX)
        printf("DEBUG: Return from update %d\n", rc);

      vers[key_num]++;
    }
  }
  else{
    /* If get...*/
    if(K_DEBUG)
      printf("DEBUG: Request is a get...\n");

    /* Decide if we're picking from hot keys */
    rc = biased_hotkey_coin();    
    if(num_keys <= locality_num)
      rc = 0;
    if ( rc == 0 ){ /* Pick from hot keys */
      if( num_keys <= locality_num)
        key_num = pick_random_key(num_keys);
      else
        key_num = pick_random_key(locality_num);
      if(K_DEBUG)
        printf("PICKING TO DO A HOT-KEY GET %d\n", key_num);
    }
    else{ /* Pick from all keys */
      key_num = pick_random_key(num_keys-locality_num) + locality_num;
      if(K_DEBUG)
        printf("PICKING TO DO A NORMAL GET %d\n", key_num);
    }

    key = keys[key_num];
    if(K_DEBUG)
      printf("DEBUG: \tkey is %s...\n", key);

    /* Do the get: no version specification */
    rc = kp_local_get(kv, key, &value, &size);
    if(K_DEBUG && rc != 0){
      printf("DEBUG: Return from get %d\n", rc);
    }

    free(value);
  }

  return rc;
}


/* Tests the latency of an average workload composed of all the
 * commands currently in our arsenal. Reports results as timing.
 * Only collects info of successful calls. 0 success, -1 failure
 * TODO: workload has weird spikes. check out return values, maybe ?
 */
int workload_evaluation(int num_iterations, kp_kv_local *kv){
  int i, rc = 0;
  kp_timers timers;

  /* Start random workload, get cost of average call */
  struct timeval *start = malloc(sizeof(struct timeval));
  struct timeval *end = malloc(sizeof(struct timeval));

  printf("***NOW DOING LATENCY TEST ON RANDOM WORKLOAD***\n");

  /* create a random workload */
  gettimeofday(start, NULL);
  start_timing(&timers);
  for (i=0; i < num_iterations; i++){
    rc = create_random_request(kv, i);
    if( (i % merge_every) == 0 ){
      rc = kp_local_merge(kv);
      if(K_DEBUG)
        printf("merging at %d\n", i);
    }
  }
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

  /* Report reuslts */
  printf("For random_workload: \n\t time elapsed %d:%d (sec:usec)"
	 "= %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n\n", 
	 num_iterations, cost_workload);
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
int latency_evaluation(int num_iterations, kp_kv_local* kv, bool overhead, 
                       bool incl_delete){
  int actual_count, key_num, i, j, rc = 0;
  void *value = NULL;
  size_t size;
  char *key = NULL;

  float cost_put_insert = 0;
  float cost_put_append = 0;
  float cost_get_current = 0;
  float cost_delete = 0;
  struct timeval *start = malloc(sizeof(struct timeval));
  struct timeval *end = malloc(sizeof(struct timeval));
  kp_timers timers;

  if(overhead)
    printf("***NOW DOING LATENCY TEST ON INDIVIDUAL FUNCTIONS (OVERHEAD)***\n");
  else
    printf("***NOW DOING LATENCY TEST ON INDIVIDUAL FUNCTIONS ***\n");

  /* Get the average cost of put-insert */
  /*=================================================================*/
  /* Meausure N iterations */
  actual_count = num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  for(i = 0; i < num_iterations; i++){
    /* pick a new key */
    key = temp_keys[i];
    if(K_DEBUG)
      printf("DEBUG: \tkey is %s...\n", key); 
    
    /* pick a value */
    value = temp_values[i];
    if(K_DEBUG)
      printf("DEBUG: \tvalue is %s...\n", (char*) value);
    
    /* Set its value */
    rc = kp_local_put(kv, key, value, value_size+1);

    /* Print out a bad return value */
    if(rc == UINT64_MAX){
      printf("DEBUG: Return from put-insert %d\n", rc);
      actual_count --;
    }

    /* Keep ourselves the key for later */      
    key_num = num_keys;
    num_keys++;
    keys[key_num] = key;
    vers[key_num] = 0;

    if( (i % merge_every) == 0 ){
      rc = kp_local_merge(kv);
      if(K_DEBUG)
        printf("merging at %d\n", i);
    }
  }
  stop_timing(&timers);
  gettimeofday(end, NULL);

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
  printf("\t iterations %d \n\t average cost %f usecs\n\n", 
	 actual_count, cost_put_insert);
  print_timing("PUT-INSERT", &timers, actual_count);


  /* Get the average cost of put-apppend */
  /*=================================================================*/
  /* Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    for(i = 0; i < num_iterations; i++){

      /* Pick an old key */
      key = keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);

      /* pick a value */
      value = temp_values[i];
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      /* Set its value */
      rc = kp_local_put(kv, key, value, value_size+1);

      /* Report bad return value */
      if(rc == UINT64_MAX){
        printf("DEBUG: Return from put-append %d\n", rc);
        actual_count --;
      }

      /* Keep ourselves the version info for later */      
      key_num = i;
      vers[key_num]++;

      if( (i % merge_every) == 0 ){
        rc = kp_local_merge(kv);
        if(K_DEBUG)
          printf("merging at %d\n", i);
      }
    }
  }
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
  cost_put_append = (float)time_usec/(float)(actual_count);

  /* Print results */
  printf("For put-append: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n\n", 
	 actual_count, cost_put_append);
  print_timing("PUT-APPEND", &timers, actual_count);


  /* Get the average cost of get-current */
  /* TODO gets aren't accurate. what figures ? */
  /*=================================================================*/
  /* no need to create values, or keys. Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    for(i = 0; i < num_iterations; i++){

      /* Pick an old key */
      key = keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
 
      /* Get its value */
      rc = kp_local_get(kv, key, &dest_values[i], (size_t* )&size);

      /* report bad return value */
      if(rc != 0){
        printf("DEBUG: Return from get %d\n", rc);
        printf("Version is current of %d\n", vers[key_num]);
        actual_count --;
      }
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

      if( (i % merge_every) == 0 ){
        rc = kp_local_merge(kv);
        if(K_DEBUG)
          printf("merging at %d\n", i);
      }

    }
  }
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
  cost_get_current = (float)time_usec/(float)(actual_count);
  
  /* Print results */
  printf("For get-current: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n\n", 
	 actual_count, cost_get_current);
  print_timing("GET-CURRENT", &timers, actual_count);

  for( i = 0; i < num_iterations; i++){
    free(dest_values[i]);
  }

  /* Get the average cost of delete */
  /* TODO remove get, this should be okay */
  /*=================================================================*/
  /* no need to create values, or keys. Measure N iterations */
  if(incl_delete){
    actual_count = MULT_FACTOR * num_iterations;
    gettimeofday(start, NULL);
    start_timing(&timers);
    for(i = 0; i < num_iterations; i++){
      
      /* Pick an old key */
      key = keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
      
      /* Get its value */
      rc = kp_local_get(kv, key, &value, (size_t* )&size);
      rc = 0;
      /* Only delete if this value actually exists ??? */
      if(rc == 0 ){
        rc = kp_local_delete_key(kv, key);
        
        /* report bad return value */
        if(rc != 0){
          printf("DEBUG: Return from delete %d\n", rc);
          actual_count --;
        }
      
        /* free the value we got, useless */
        free(value);
        
      } else{
        /* report bad get value */
        printf("UHOH! key we're trying to delete doesn't have current val\n");
        actual_count --;
      	if(K_DEBUG)
          printf("DEBUG: \tvalue is %s...\n", (char*) value);
      }

      if( (i % merge_every) == 0 ){
        rc = kp_local_merge(kv);
        if(K_DEBUG)
          printf("merging at %d\n", i);
      }

    }
    stop_timing(&timers);
    gettimeofday(end, NULL);
    
    /* Figure out timing info */
    sec = end->tv_sec - start->tv_sec;
    usec = end->tv_usec - start->tv_usec;
    if(end->tv_usec < start->tv_usec){
      sec --;
      usec += 1000000;
    }
    time_usec = 1000000*sec+usec;
    cost_delete = (float)time_usec/(float)(actual_count);
    
    /* Print results */
    printf("For delete-key: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
           sec, usec, time_usec);
    printf("\t iterations %d \n\t average cost %f usecs\n\n", 
           actual_count, cost_delete);
    print_timing("DELETE-KEY", &timers, actual_count);
  }

  /*=================================================================*/

  /* Free the kv store, reset counts, and free all those values */
  free(start);
  free(end);

  return rc;
}


/* Tests the overhead of put (insert and append) operations over
 * a straight memcopy of the relevant data. Reports overhead in size
 * and percentage.  0 success, -1 failure
 * TODO memcpy values suspicious?
 */
int memcpy_malloc_evaluation(int num_iterations, bool free_me){
  int actual_count, i,j, rc = 0;
  void *value = NULL;
  char *key = NULL;
  kp_timers timers;

  float overhead_base = 0;
  struct timeval *start = malloc(sizeof(struct timeval));
  struct timeval *end = malloc(sizeof(struct timeval));

  printf("***NOW DOING OVERHEAD EVALUATION ON INDIVIDUAL FUNCTIONS***\n");

  int usec, sec;
  int time_usec;


  /* Compare to averare cost of memcopy-free the same value size */
  /*=================================================================*/

  /* Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations * 10;
  gettimeofday(start, NULL);
  start_timing(&timers);
  for(j = 0; j < 10*MULT_FACTOR; j++){    //pump up the iterations number 
    for(i = 0; i < num_iterations; i++){

      /* pick a value */
      value = temp_values[i];
      if(K_DEBUG){
        printf("DEBUG: \tvalue is %s...\n", (char*) value);
      }      

      /* Malloc for size. Copy its value */
      second_values[i] = malloc(value_size+1);
      memcpy(second_values[i], value, value_size+1);
      if(free_me)
        free(second_values[i]); // we actually also free in our get test 

      /* Report bad return values ???*/
    }
  }
  stop_timing(&timers);
  gettimeofday(end, NULL);

  /* Figure out timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  overhead_base = (float)time_usec/(float)(actual_count);
  
  /* print results */
  if(free_me){
    printf("For memcpy/malloc/free value: \n\t time elapsed %d:%d (sec:usec)"
           "= %d usecs \n",
           sec, usec, time_usec);
  }
  else{
    printf("For memcpy/malloc(NO FREE) value: \n\t time elapsed %d:%d (sec:usec)"
           "= %d usecs \n",
           sec, usec, time_usec);
  }
  printf("\t iterations %d \n\t average cost %f usecs\n", 
	 actual_count, overhead_base);
  print_timing("MEMCPY-MALLOC-VALUE", &timers, actual_count);

  /* Compare to average cost of memcopy-no-free the same KEY size */
  /*=================================================================*/

  /* Measure N iterations */
  actual_count = 10 * MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  start_timing(&timers);
  for(j = 0; j < 10*MULT_FACTOR; j++){    //pump up iterations numbers
    for(i = 0; i < num_iterations; i++){

      /* Pick an old key */
      key = temp_keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
      
      /* Copy its value */
      second_keys[i] = malloc(key_size+1);
      memcpy(second_keys[i], key, key_size+1);
      if (free_me)
        free(second_keys[i]); // we actually also free in our get test       

      /* report bad return values ??? */
    }
  }
  stop_timing(&timers);
  gettimeofday(end, NULL);

  /* Figure out timing */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  overhead_base = (float)time_usec/(float)(actual_count);

  /* Print results */
  if(free_me){
    printf("For memcpy/malloc/free KEY: \n\t time elapsed %d:%d (sec:usec)"
           "= %d usecs \n",
           sec, usec, time_usec);
  }
  else{
    printf("For memcpy/malloc(NO FREE) KEY: \n\t time elapsed %d:%d (sec:usec)"
           "= %d usecs \n",
           sec, usec, time_usec);
  }
  printf("\t iterations %d \n\t average cost %f usecs\n", 
	 actual_count, overhead_base);
  print_timing("MEMCPY-MALLOC-KEY", &timers, actual_count);

  /* Misc values free */
  free(start);
  free(end);

  if(!free_me){
    for(i = 0; i <  num_iterations; i++){
      free(second_values[i]);
      free(second_keys[i]);
    }
  }
  return rc;
}



/* Calls the key-value store internal measurement function
 * to evaluate the size and proportions of the store
 * Works up some random workload first. 
 */
int local_keyvalue_size(int num_iterations, kp_kv_local *kv){
  int i, rc = 0;

  printf("***BEGINNING MEMORY FOOTPRINT EVALUATION***\n");  

  /* create a random workload */
  for (i=0; i < num_iterations; i++){
    rc = create_random_request(kv, i);
  }

  /* Create file name*/
  char* buf = malloc(32);
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
  int rc = 0;
  char *ret_string;
  unsigned long tid;
  worker_thread_args *thread_args;
  kp_kv_master *master;
  kp_kv_local *local;
  
  /* setup thread */
  tid = pthread_self();
  thread_args = (worker_thread_args *) arg;
  master = thread_args->master;
  rc = kp_kv_local_create(master, &local);
  if(rc != 0){
    printf("thread_%lu: kp_kv_local_create() returned error=%d\n", tid, rc);
    return NULL;
  }

  /* Do work */
  printf("Hi, I'm thread/worker %lu starting work\n", tid);
  while(true){
    int i;
    for (i=0; i < iterations; i++){
      create_random_request(local, i);
    }
  }

  /* cleanup and return */
  rc = kp_kv_local_destroy(local);
  if(rc != 0){
    kp_error("thread_%lu: kp_kv_local_destroy() returned error=%d\n",
             tid, rc);
    return NULL;
  }
  ret_string = malloc(RET_STRING_LEN);
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
  worker_thread_args thread_args[MAX_THREADS];  
  kp_kv_master *master;

  /* Check args */
  if(num_threads <= 0){
    kp_die("invalid num_threads=%d\n", num_threads);
  }
  kp_debug("pid=%d, num_threads=%d\n", getpid(), num_threads);


  /* Create the master */
  rc = kp_kv_master_create(&master, MASTER_CONSISTENCY_MODE);
  if(rc != 0){
    kp_die("kp_kv_master_create() failed\n");
  }
  if(K_DEBUG){
    printf("kp_kv_master_create() succeeded, consistency mode=%s\n",
           consistency_mode_to_string(kp_master_get_mode(master)));
  }

  /* Create the one thread */
  for (i=0; i < num_threads; i++){
    cpu = CPU_OFFSET + (i % NUM_CPUS);
    CPU_ZERO(&(thread_args[i]).cpu_set);
    CPU_SET(cpu, &(thread_args[i]).cpu_set);
    thread_args[i].master = master;
    if(K_DEBUG){
      printf("Creating threads to complete workload MT tasks\n");
    }
    
    /* Set attributes and spawn */
    rc = pthread_attr_init(&attr);
    if(rc != 0)
      kp_die("pthread_attr_init() returned error=%d\n", rc);
    
    rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t),
                                     &((thread_args[i]).cpu_set));
    if(rc != 0)
      kp_die("pthread_attr_setaffinity_np() returned error=%d\n", rc);
    
    rc = pthread_create(&threads[i], &attr, &worker_thread_entrypoint, 
                        (void *)(&(thread_args[i])));
    if(rc != 0)
      kp_die("pthread_create() returned error=%d\n", rc);
    
    rc = pthread_attr_destroy(&attr);
    if(rc != 0)
      kp_die("pthread_attr_destroy() returned error=%d\n", rc);
  }

  /*=============== MASTER ACTIONS ==================== */

  /* measure individual response time... but we want it on a local one... */
  //  rc = latency_evaluation(iterations, &master, false, false);

  /* measure workload response time */


  
  /* Get that worker back! */
  for( i = 0; i < num_threads; i++){
    rc = pthread_join(threads[i], &ret_thread);
    if(rc != 0)
      kp_die("pthread_join() returned error=%d\n", rc);
    if(ret_thread)
      free(ret_thread);
  }

  printf("***CONCLUDING THREAD EVALUATION***\n");
  /* Cleanup */
  rc = kp_kv_master_destroy(master);
  if (rc != 0){
    kp_die("kp_kv_master_destroy() failed\n");
  }

  /* WTF is this?? */
  for(i = 0; i < num_keys; i++){
    if(keys[i] == NULL)
      printf("missing key for %d\n", i);
    if(vers[i] < 0 )
      printf("missing vers for %d\n", i);
  }

  reset_counts();

  return;

}


/* Packages up single threaded evaluations so we can use it from within
   a single worker setup */
void *little_worker_thread_start(void *arg){
  int rc = 0;
  char *ret_string;
  unsigned long tid;
  worker_thread_args *thread_args;
  kp_kv_master *master;
  kp_kv_local *local;
  
  /* setup thread */
  tid = pthread_self();
  thread_args = (worker_thread_args *) arg;
  master = thread_args->master;


  /* Do work */

  /* Local worker ONE */
  rc = kp_kv_local_create(master, &local);
  if(rc != 0){
    printf("thread_%lu: kp_kv_local_create() returned error=%d\n", tid, rc);
    return NULL;
  }

  rc = latency_evaluation(iterations, local, false, false);
  rc = memcpy_malloc_evaluation(iterations, false);

  rc = kp_kv_local_destroy(local);
  if(rc != 0){
    kp_error("thread_%lu: kp_kv_local_destroy() returned error=%d\n",
             tid, rc);
    return NULL;
  }
  reset_counts();

  /* Local worker TWO */
  rc = kp_kv_local_create(master, &local);
  if(rc != 0){
    printf("thread_%lu: kp_kv_local_create() returned error=%d\n", tid, rc);
    return NULL;
  }

  rc = workload_evaluation(iterations, local);

  rc = kp_kv_local_destroy(local);
  if(rc != 0){
    kp_error("thread_%lu: kp_kv_local_destroy() returned error=%d\n",
             tid, rc);
    return NULL;
  }
  reset_counts();


  /* Local worker THREE */
  /* 
  rc = kp_kv_local_create(master, &local);
  if(rc != 0){
    printf("thread_%lu: kp_kv_local_create() returned error=%d\n", tid, rc);
    return NULL;
  }

  rc = local_keyvalue_size(iterations, local);

  rc = kp_kv_local_destroy(local);
  if(rc != 0){
    kp_error("thread_%lu: kp_kv_local_destroy() returned error=%d\n",
             tid, rc);
    return NULL;
    }
    reset_counts();
  */


  /* cleanup and return */
  ret_string = malloc(RET_STRING_LEN);
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
  worker_thread_args thread_args;
  kp_kv_master *master;

  printf("\n*** BEGINING SINGLE THREADED TESTS***\n\n");
  /* Create the master */
  rc = kp_kv_master_create(&master, MASTER_CONSISTENCY_MODE);
  if(rc != 0){
    kp_die("kp_kv_master_create() failed\n");
  }
  if(K_DEBUG){
    printf("kp_kv_master_create() succeeded, consistency mode=%s\n",
           consistency_mode_to_string(kp_master_get_mode(master)));
  }

  /* Create the one thread */
  cpu = CPU_OFFSET + (i % NUM_CPUS);
  CPU_ZERO(&(thread_args.cpu_set));
  CPU_SET(cpu, &(thread_args.cpu_set));
  thread_args.master = master;
  if(K_DEBUG){
    printf("Creating single thread to complete single-threaded tasks\n");
  }
   
  /* Set attributes and spawn */
  rc = pthread_attr_init(&attr);
  if(rc != 0)
    kp_die("pthread_attr_init() returned error=%d\n", rc);

  rc = pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t),
                                   &(thread_args.cpu_set));
  if(rc != 0)
    kp_die("pthread_attr_setaffinity_np() returned error=%d\n", rc);

  rc = pthread_create(&thread, &attr, &little_worker_thread_start, 
                      (void *)(&thread_args));
  if(rc != 0)
    kp_die("pthread_create() returned error=%d\n", rc);

  rc = pthread_attr_destroy(&attr);
  if(rc != 0)
    kp_die("pthread_attr_destroy() returned error=%d\n", rc);

  
  /* Get that worker back! */
  rc = pthread_join(thread, &ret_thread);
  if(rc != 0)
    kp_die("pthread_join() returned error=%d\n", rc);
  if(ret_thread)
    free(ret_thread);

  /* Cleanup */
  rc = kp_kv_master_destroy(master);
  if (rc != 0){
    kp_die("kp_kv_master_destroy() failed\n");
  }

  printf("\n*** CONCLUDING SINGLE THREADED TESTS***\n\n");

  return;

}


/* Sets this process' affinity to run on only a single CPU (CPU 1, i.e.
 * the second CPU...).
 */
void set_process_affinity()
{
  int ret;
  cpu_set_t mask;

  CPU_ZERO(&mask);
  CPU_SET(0x1, &mask);  //add CPU 1 to mask

  ret = sched_setaffinity(0, sizeof(cpu_set_t), &mask);
  if (ret != 0) {
    printf("ERROR: sched_setaffinity() returned %d\n", ret);
    abort();
  }

  printf("Set CPU affinity of this process to CPU 1\n");
}


int main(int argc, char *argv[])
{
  //  int num_threads = 4;
  int rc = 0;
  int i;
  
  /* Read in parameters from command line */
  if(argc != 10){
    printf("USAGE: %s <iters> <ksize> <vsize> <merge_every> <put_prob>"
	   " <update_prob> <gc_policy> <locality_prob> <locality_num>\n", argv[0]);
    exit(-1);
  }

  /* Get integer parameters */
  iterations = atoi(argv[1]);    //1
  key_size = atoi(argv[2]);      //2
  value_size = atoi(argv[3]);    //3
  merge_every = atoi(argv[4]);   //4

  /* TODO add capability to alter these */
  put_probability = .4;          //5
  update_probability = .7;       //6
  care_about_probability = 1.0;  //7
  locality_probability =0.2;     //8
  locality_num = atoi(argv[9]);  //9

  printf("NUM HOT KEYS =%d\n", locality_num);

  /*--------------------------------------------------------------*/
  set_process_affinity();   /* Set process affinity: */
  srand(time(NULL));        /* Seed your random number generator */

  /* Do global allocations */
  keys = malloc(MAX_KV_SIZE*sizeof(char*));
  vers = malloc(MAX_KV_SIZE*sizeof(int));

  /* Do allocation/creation of random keys and values */
  temp_keys = malloc(MAX_KV_SIZE*sizeof(char*));
  temp_values = malloc(MAX_KV_SIZE*sizeof(void*));
  dest_values = malloc(MAX_KV_SIZE*sizeof(void*));
  for(i = 0; i < MAX_KV_SIZE; i++){
    temp_keys[i] = malloc(key_size+1);
    rc = create_random_key(key_size, temp_keys[i]);

    temp_values[i] = malloc(value_size+1);
    rc = create_random_value(value_size, temp_values[i]);
  }
  printf("Created %d random keys and values\n", MAX_KV_SIZE);

  /* Do allocation of buffers for memcpy tests */
  second_values = malloc(MAX_KV_SIZE*sizeof(void*));
  second_keys = malloc(MAX_KV_SIZE*sizeof(void*));

  /*--------------------------------------------------------------*/
  /* Single-threaded evaluations */
  single_worker_test();

  
  /* Threaded evaluation for bottleneck */
  //  num_threads = 4;
  //  ramp_up_threads(num_threads);

  /*--------------------------------------------------------------*/
  /* Free global allocations */
  free(keys);
  free(vers);

  /* Free random keys and values */
  free(temp_keys);
  free(temp_values);
  free(dest_values);

  /* Free buffers for memcpy tests */
  free(second_values);
  free(second_keys);


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

