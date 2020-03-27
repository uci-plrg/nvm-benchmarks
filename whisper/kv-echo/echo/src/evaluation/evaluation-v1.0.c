/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: nil
 * vi: set shiftwidth=2 tabstop=2 expandtab:
 * :indentSize=2:tabSize=2:noTabs=true:
 *
 * Katelin Bailey & Peter Hornyack
 * 8/28/2011
 */

#include "../include/kp_kvstore.h"
#include "kp_macros.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <sched.h>

#define K_DEBUG 0
#define MAX_KV_SIZE 1000000
#define MULT_FACTOR 3

/* Parameters that vary the evaluation */
int iterations = 100000;            // Number of iterations to get average
int key_size = 6;
int value_size = 20;                  // Uniform size for all values
float put_probability = .4;          // Probability that next request is a put
float update_probability = .7;       // Prob. that next put is to existing val
float care_about_probability = 1.0;  // Prob. that this is a value we keep
float locality_probability =0.0;     // Prob. that this request is a hot req.


/* Keep track of some keys so we can run random workload */
int num_keys = 0;                    // For an instance, num unique keys in set
char **keys;                         // Keys (stored for updates)
int *vers;                           // How many versions per key (for gets)

/* Do some block allocations at initialization */
void **dest_values;                  // Generate a gazillion potential values 
void **temp_values;                  // Generate a gazillion potential values 
char **temp_keys;                    // Gemerate a gazillion potential keys 
void **second_values;                // Space for malloc copies to go
void **second_keys;                // Space for malloc copies to go
pthread_mutex_t key_lock;

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
  int ret;

  ret = clock_gettime(CLOCK_REALTIME, &(timers->realtime_start));
  if (ret != 0) {
    printf("ERROR: start clock_gettime(CLOCK_REALTIME) returned %d\n", ret);
  }

  ret = clock_gettime(CLOCK_MONOTONIC, &(timers->monotonic_start));
  if (ret != 0) {
    printf("ERROR: start clock_gettime(CLOCK_MONOTONIC) returned %d\n", ret);
  }

  ret = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &(timers->proc_cputime_start));
  if (ret != 0) {
    printf("ERROR: start clock_gettime(CLOCK_PROCESS_CPUTIME_ID) returned %d\n",
        ret);
  }
}

/* Stops various timers. The kp_timers pointer that is passed to
 * this function must have been passed to start_timing() before
 * it is used here, or undefined behavior may result.
 */
void stop_timing(kp_timers *timers) {
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
  struct timespec ts_diff;
  unsigned long total_usec;
  float avg_usec;

  ts_subtract(&(timers->realtime_stop), &(timers->realtime_start), &ts_diff);
  //printf("%s: (sec:nsec): start %d:%ld ; stop %d:%ld ; diff %d:%ld \n",
  //    prefix,
  //    (int)((timers->realtime_start).tv_sec), (timers->realtime_start).tv_nsec,
  //    (int)((timers->realtime_stop).tv_sec), (timers->realtime_stop).tv_nsec,
  //    (int)(ts_diff.tv_sec), ts_diff.tv_nsec);
  total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
  avg_usec = (float)total_usec / (float)divisor;
  //printf("%s: \n\t CLOCK_REALTIME elapsed %d:%ld (sec:nsec) = %lu usec \n",
  //    prefix, (int)ts_diff.tv_sec, ts_diff.tv_nsec, total_usec);
  //printf("\t iterations %d \n\t average cost %f usecs\n\n",
  //    divisor, avg_usec);
  printf("%s: \n\t CLOCK_REALTIME \t %lu usec, average cost %f usec\n",
      prefix, total_usec, avg_usec);

  ts_subtract(&(timers->monotonic_stop), &(timers->monotonic_start), &ts_diff);
  total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
  avg_usec = (float)total_usec / (float)divisor;
  //printf("%s: \n\t CLOCK_MONOTONIC elapsed %d:%ld (sec:nsec) = %lu usec \n",
  //    prefix, (int)ts_diff.tv_sec, ts_diff.tv_nsec, total_usec);
  //printf("\t iterations %d \n\t average cost %f usecs\n\n",
  //    divisor, avg_usec);
  printf("\t CLOCK_MONOTONIC \t %lu usec, average cost %f usec\n",
      total_usec, avg_usec);

  ts_subtract(&(timers->proc_cputime_stop), &(timers->proc_cputime_start), &ts_diff);
  total_usec = (ts_diff.tv_sec)*1000000 + (ts_diff.tv_nsec)/1000;
  avg_usec = (float)total_usec / (float)divisor;
  //printf("%s: \n\t CLOCK_PROCESS_CPUTIME_ID elapsed %d:%ld (sec:nsec) = %lu usec \n",
  //    prefix, (int)ts_diff.tv_sec, ts_diff.tv_nsec, total_usec);
  //printf("\t iterations %d \n\t average cost %f usecs\n\n",
  //    divisor, avg_usec);
  printf("\t CLOCK_PROC_CPUTIME_ID \t %lu usec, average cost %f usec\n",
      total_usec, avg_usec);

  printf("\n");
}

void free_all_keys(void){
  int i;
  for(i = 0; i < num_keys; i++){
    if(keys[i] != NULL){
      free(keys[i]);
      keys[i] = NULL;
    }
  }
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
int pick_random_key(void){
  int r = rand();
  float rd = (float)r/(float)RAND_MAX ;
  r = rd*num_keys;
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
    sprintf(key, "%s%c", key, c);
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
int create_random_request(kp_kvstore *kv, int i){
  int key_num, version, rc = 0;
  void *value = NULL;
  size_t size;
  char *key = NULL;
  
  /* Pick put or get */
  rc = biased_put_get_coin();
  if((num_keys < MAX_KV_SIZE) && ((num_keys == 0) || (rc == 0))){
    if(K_DEBUG)
      printf("DEBUG: Request is a put...\n");

    /* If put, pick update or append */
    rc = biased_update_coin();
    if((num_keys == 0) | (rc == 0)){

      if(K_DEBUG)
        printf("DEBUG: \tof a new key %d...\n", key_num);

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
      rc = kp_put(kv, key, value, value_size+1);

      if(K_DEBUG && rc == UINT64_MAX)
        printf("DEBUG: Return from put %d\n", rc);
      
      // TODO find out which aren't returning right
      if(rc == UINT64_MAX)
        printf("DEBUG: Return from put %d\n", rc);
    }
    else{
      if(K_DEBUG)
        printf("DEBUG: \tof an old key...\n");
      /* TODO Determine Locality factor */
      

      /* Pick a key */
      key_num = pick_random_key();
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
      rc = kp_put(kv, key, value, value_size+1);

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

    /* TODO Determine Locality factor */


    /* Pick a key */
    key_num = pick_random_key();
    key = keys[key_num];
    if(K_DEBUG)
      printf("DEBUG: \tkey is %s...\n", key);

    /* Pick a version */
    version = pick_random_version(vers[key_num]);
    if(K_DEBUG)
      printf("DEBUG: \tversion is %d...\n", version);

    rc = kp_get_version_local(kv, key, version, &value, &size);
    if(K_DEBUG && rc != 0){
      printf("DEBUG: Return from get %d\n", rc);
      printf("Version is %d of %d\n", version, vers[key_num]);
    }

    // TODO figure out which gets aren't returning right
    if(rc != 0){
      printf("DEBUG: Return from get %d\n", rc);
      printf("Version is %d of %d\n", version, vers[key_num]);
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
int workload_evaluation(int num_iterations, kp_kvstore *kv){
  int i, rc = 0;
  bool kv_created = false;
  kp_timers timers;

  /* Start random workload, get cost of average call */
  struct timeval *start = malloc(sizeof(struct timeval));
  struct timeval *end = malloc(sizeof(struct timeval));

  printf("***NOW DOING LATENCY TEST ON RANDOM WORKLOAD***\n");

  /* Create a kv store */
  if(kv == NULL){
    kv_created = true;
    kv = kp_kvstore_create();
    reset_counts();
  }

  /* create a random workload */
  gettimeofday(start, NULL);
  start_timing(&timers);
  for (i=0; i < num_iterations; i++){
    rc = create_random_request(kv, i);
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


  /* Cleanup keyvaluestore and inserted keys */
  if(kv_created){
    kp_kvstore_destroy(kv);
    reset_counts();
  }
  free(start);
  free(end);
  return rc;
}

/* Tests the latency of each operation currently in our arsenal
 * put-insert, put-append, get, delete, garbage collect.
 * Reports the latency as timing information. 
 * Only collects info on successful calls. 0 success, -1 failure
 */
int latency_evaluation(int num_iterations, kp_kvstore* kv, bool overhead, 
                       bool incl_delete){
  int actual_count, key_num, i, j, rc = 0;
  void *value = NULL;
  size_t size;
  char *key = NULL;

  float cost_put_insert = 0;
  float cost_put_append = 0;
  float cost_get_current = 0;
  float cost_get_any = 0;
  float cost_delete = 0;
  struct timeval *start = malloc(sizeof(struct timeval));
  struct timeval *end = malloc(sizeof(struct timeval));
  kp_timers timers;

  if(overhead)
    printf("***NOW DOING LATENCY TEST ON INDIVIDUAL FUNCTIONS (OVERHEAD)***\n");
  else
    printf("***NOW DOING LATENCY TEST ON INDIVIDUAL FUNCTIONS ***\n");
  bool kv_created = false;

  /* create a new kv if needed */
  if(kv == NULL){
    if(K_DEBUG)
      printf("DEBUG: Need to create a new kvstore for workload\n");
    kv = kp_kvstore_create();
    kv_created = true;
    reset_counts();
  }


  /* Get the average cost of put-insert */
  /*=================================================================*/
  /* Meausure N iterations */
  actual_count = num_iterations;
  gettimeofday(start, NULL);
  //  start_timing(&timers);
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
    rc = kp_put(kv, key, value, value_size+1);

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
  }
  //  stop_timing(&timers);
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
  //  print_timing("PUT-INSERT", &timers, actual_count);


  /* Get the average cost of put-apppend */
  /*=================================================================*/
  /* Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  //  start_timing(&timers);
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
      rc = kp_put(kv, key, value, value_size+1);

      /* Report bad return value */
      if(rc == UINT64_MAX){
        printf("DEBUG: Return from put-append %d\n", rc);
        actual_count --;
      }

    /* Keep ourselves the version info for later */      
    key_num = i;
    vers[key_num]++;
    }
  }
  //  stop_timing(&timers);
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
  //  print_timing("PUT-APPEND", &timers, actual_count);


  /* Get the average cost of get-current */
  /* TODO gets aren't accurate. what figures ? */
  /*=================================================================*/
  /* no need to create values, or keys. Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  //  start_timing(&timers);
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    for(i = 0; i < num_iterations; i++){

      /* Pick an old key */
      key = keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
 
      /* Get its value */
      rc = kp_get(kv, key, &dest_values[i], (size_t* )&size);

      /* report bad return value */
      if(rc != 0){
        printf("DEBUG: Return from get %d\n", rc);
        printf("Version is current of %d\n", vers[key_num]);
        actual_count --;
      }
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

    }
  }
  //  stop_timing(&timers);
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
  //  print_timing("GET-CURRENT", &timers, actual_count);

  for( i = 0; i < num_iterations; i++){
    free(dest_values[i]);
  }

  /* Get the average cost of get-any */
  /* TODO gets aren't accurate. what figures ? */
  /*=================================================================*/
  /* no need to create values, or keys. Measure N iterations */
  actual_count = MULT_FACTOR * num_iterations;
  gettimeofday(start, NULL);
  //  start_timing(&timers);
  for(j = 0; j < MULT_FACTOR; j++){    //pump up iterations number
    for(i = 0; i < num_iterations; i++){

      /* Pick an old key */
      key = keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
 
      /* Get its value */
      rc = kp_get_version_local(kv, key, (uint64_t) 0, 
				&dest_values[i], (size_t* )&size);

      /* Report bad return value */
      if(rc != 0){
        printf("DEBUG: Return from get %d\n", rc);
        printf("Version is 0 of %d\n", vers[key_num]);
        actual_count --;
      }
      if(K_DEBUG)
        printf("DEBUG: \tvalue is %s...\n", (char*) value);

    }
  }
  //  stop_timing(&timers);
  gettimeofday(end, NULL);
  
  /* figure out timing info */
  sec = end->tv_sec - start->tv_sec;
  usec = end->tv_usec - start->tv_usec;
  if(end->tv_usec < start->tv_usec){
    sec --;
    usec += 1000000;
  }
  time_usec = 1000000*sec+usec;
  cost_get_any = (float)time_usec/(float)(actual_count);

  /* print results */
  printf("For get-any: \n\t time elapsed %d:%d (sec:usec) = %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n\n", 
	 actual_count, cost_get_any);
  //  print_timing("GET-ANY", &timers, actual_count);

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
    //    start_timing(&timers);
    for(i = 0; i < num_iterations; i++){
      
      /* Pick an old key */
      key = keys[i];
      if(K_DEBUG)
        printf("DEBUG: \tkey is %s...\n", key);
      
      /* Get its value */
      rc = kp_get(kv, key, &value, (size_t* )&size);
      rc = 0;
      /* Only delete if this value actually exists ??? */
      if(rc == 0 ){
        rc = kp_delete_key(kv, key);
        
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
    }
    //    stop_timing(&timers);
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
    //    print_timing("DELETE-KEY", &timers, actual_count);
  }

  /*=================================================================*/

  /* Free the kv store, reset counts, and free all those values */
  if(kv_created){
    kp_kvstore_destroy(kv);
    reset_counts();
  }
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
int keyvalue_size(int num_iterations, kp_kvstore *kv){
  int i, rc = 0;
  bool created = false;

  if(kv == NULL){
    kv = kp_kvstore_create();
    created = true;
    reset_counts();
  }
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
  kp_print_stats(kv, fp, false);

  /* Free all the temp values and keys, as well as un-used keys */
  if(created){
    kp_kvstore_destroy(kv);
    reset_counts();
  }

  /* Close files, destroy kv, clean up the rest... */
  fclose(fp); 
  free(buf); 
  printf("***FINISHING MEMORY FOOTPRINT EVALUATION***\n");  
  return rc;
}




/* Evaluates the garbage collection mechanism as compared to the uncollected
 * version. Gets base cost on empty key value store, then populates the store
 * and gets GC cost repeatedly, after different intervals/sizes. 
 * reports a range of values.
 */
int evaluate_gc(int num_iterations){
  num_iterations = 1000;
  int value_size, rc = 0;
  char *key;
  char *value;
  kp_kvstore *kv = kp_kvstore_create();
  int actual_count, i,j;
  kp_timers timers;

  float overhead_base = 0;
  struct timeval *start = malloc(sizeof(struct timeval));
  struct timeval *end = malloc(sizeof(struct timeval));

  int usec, sec;
  int time_usec;

  printf("***BEGINNING GARBAGE COLLECTION EVALUATION***\n");  

  /* Quick check that the policy is working as stated */
  kp_gc_set_global_kept(kv, 2);
  kp_gc_set_local_kept(kv, 1);

  value_size = 2;
  key = "key1";
  value = "a";
  rc = kp_put(kv, key, (void*) value, value_size);
  key = "key1";
  value = "b";
  rc = kp_put(kv, key, (void*) value, value_size);
  key = "key2";
  value = "c";
  rc = kp_put(kv, key, (void*) value, value_size);
  key = "key3";
  value = "d";
  rc = kp_put(kv, key, (void*) value, value_size);
  key = "key1";
  value = "e";
  rc = kp_put(kv, key, (void*) value, value_size);
  key = "key3";
  value = "f";
  rc = kp_put(kv, key, (void*) value, value_size);

  rc = kp_run_gc(kv);
  printf("kp_run_gc() returned %d\n", rc);

  key = "key2";
  value = "g";
  rc = kp_put(kv, key, (void*) value, value_size);
  key = "key1";
  value = "h";
  rc = kp_put(kv, key, (void*) value, value_size);
  key = "key3";
  value = "i";
  rc = kp_put(kv, key, (void*) value, value_size);

  rc = kp_run_gc(kv);
  printf("kp_run_gc() returned %d\n", rc);
  kp_kvstore_destroy(kv);
  
  /* Do garbage collection on an empty kv. Check cost */
  /*=================================================================*/
  kv = kp_kvstore_create();
  actual_count = num_iterations*MULT_FACTOR;
  gettimeofday(start, NULL);
  start_timing(&timers);
  for( j = 0; j < MULT_FACTOR; j++){
    for(i = 0; i < num_iterations; i++){
      rc = kp_run_gc(kv);
    }
  }
  stop_timing(&timers);
  gettimeofday(end, NULL);
  printf("kp_run_gc() returned %d\n", rc);

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
  printf("For garbage collection on empty: \n\t time elapsed %d:%d (sec:usec)"
	 "= %d usecs \n",
	 sec, usec, time_usec);
  printf("\t iterations %d \n\t average cost %f usecs\n", 
	 actual_count, overhead_base);
  print_timing("GARBAGE-COLLECTION-EMPTY", &timers, actual_count);

  /*=================================================================*/
  /* For... a while */
  kp_gc_set_global_kept(kv, 2000);
  kp_gc_set_local_kept(kv, 1);
  int k;
  for( k = 1; k <= 1; k++){
    /* Work up some load */
    rc = workload_evaluation(k*iterations, kv);

    /* Maybe do a latency evaluation */
    rc = latency_evaluation(iterations, kv, false, false);

    /* Dump out that memory */
    //    rc = keyvalue_size(iterations, kv);

    /* Get timing on garbage collection */
    actual_count = 1;;
    gettimeofday(start, NULL);
    start_timing(&timers);
    rc = kp_run_gc(kv);
    stop_timing(&timers);
    gettimeofday(end, NULL);
    printf("kp_run_gc() returned %d\n", rc);
    
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
    printf("For garbage collection on %d workload: \n\t time elapsed "
	   "%d:%d (sec:usec) = %d usecs \n",
	   k, sec, usec, time_usec);
    printf("\t iterations %d \n\t average cost %f usecs\n", 
	   actual_count, overhead_base);
    print_timing("GARBAGE-COLLECTION-WORKLOAD", &timers, actual_count);
    
    /* Dump out the GC'd memory */
    //    rc = keyvalue_size(iterations, kv);
  }

  return 0;
}




/* For the thread-test evaluations, keep each not-main thread
 * busy with a random workload constantly, to maximize numbers
 */
void *thread_entrypoint(void *arg){
  kp_kvstore *kv = (kp_kvstore *)arg;
  while(true){
    //    printf("this is a worker thread, iterating random workload %d times\n",
    //	   iterations);
    /* create a random workload */
    int i;
    for (i=0; i < iterations; i++){
      create_random_request(kv, i);
    }
  }

  return NULL;
}


/* Progressively increases the number of threads such that
 * we can find the bottle neck. After each increase in threads
 * measures the latency of each request type, and the average
 * latency of a random workload. Individual functions print results.
 */
void ramp_up_threads(int max_threads){
  pthread_t my_threads[max_threads];
  int i, rc = 0;

  /* Create a single key_value store */
  kp_kvstore *kv = kp_kvstore_create();
  printf("***BEGINNING THREAD EVALUATION***\n");
  /* Send threads at it */
  for (i=0; i < max_threads; i++){

    /* create a new thread */
    rc = pthread_create(&my_threads[i], NULL, &thread_entrypoint, (void *) kv);
    if (rc != 0){
      printf("ERROR: problem creating threads at %d threads. Exiting...\n", i);
      /* cancel threads*/
      return;
    }
    if(K_DEBUG)
      printf("New thread %d\n", i);


    if(i == 0 || i == 1 || i == 3 || i == 7 || i == 15 || i == 31 || i == 63 ||
       i == 127 || i == 255 || i == 511 || i == 1023 || i == 2047 ){
      printf("# Threads is %d\n", i+1);
      /* Do some random work */
      //    printf("this is your main thread, iterating random workload %d times\n",
      // iterations);
      //      int j;
      //      for (j=0; j < iterations; j++){
      //        create_random_request(kv, j);
      //      }

      //  rc = latency_evaluation(iterations, kv, false, false);
      /* measure individual response time */
      /* measure workload response time */
    }
  }

  rc = latency_evaluation(iterations, kv, false, false);
      

  printf("***CONCLUDING THREAD EVALUATION***\n");
  /* Free the store, cancel the threads */
  for(i = 0; i< max_threads; i++){
    rc = pthread_cancel(my_threads[i]);
    if(K_DEBUG){
      if(rc != 0)
        printf("Problem cancelling thread %d\n", i);
      else
        printf("Successfully cancelled thread %d\n", i);
    }
  }

  /* WTF is this?? */
  for(i = 0; i < num_keys; i++){
    if(keys[i] == NULL)
      printf("missing key for %d\n", i);
    if(vers[i] < 0 )
      printf("missing vers for %d\n", i);
  }

  kp_kvstore_destroy(kv);
  reset_counts();

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

  //printf("CPU_COUNT() of set: %d\n", CPU_COUNT(&mask));
  printf("Set CPU affinity of this process to CPU 1\n");
}


int main(int argc, char *argv[])
{
  int max_threads = 256;
  int rc = 0;
  
  /* Read in parameters from command line */
  if(argc != 8){
    printf("USAGE: %s <iters> <ksize> <vsize> <put_prob> <update_prob>"
	   " <gc_policy> <locality_prob>\n", argv[0]);
    exit(-1);
  }

  /* Get integer parameters */
  iterations = atoi(argv[1]);
  key_size = atoi(argv[2]);
  value_size = atoi(argv[3]);

  /* TODO add capability to alter these */
  put_probability = .4;          
  update_probability = .7;       
  care_about_probability = 1.0;  
  locality_probability =0.0;

  /* Set process affinity: */
  set_process_affinity();

  /* Seed your random number generator */
  srand(time(NULL));

  /* Do global allocations */
  pthread_mutex_init(&key_lock, NULL);
  keys = malloc(MAX_KV_SIZE*sizeof(char*));
  vers = malloc(MAX_KV_SIZE*sizeof(int));

  /* Do allocation/creation of random keys and values */
  int i;
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

  /* Single-threaded evaluations */
  //rc = latency_evaluation(iterations, NULL, false, false);
  //  rc = memcpy_malloc_evaluation(iterations, false);
  //  rc = workload_evaluation(iterations, NULL);
  //  rc = keyvalue_size(iterations, NULL);
  //rc = evaluate_gc(iterations);

  /* Threaded evaluation for bottleneck */
  max_threads = 4;
  ramp_up_threads(max_threads);
  max_threads = 8;
  ramp_up_threads(max_threads);
  max_threads = 16;
  ramp_up_threads(max_threads);
  max_threads = 32;
  ramp_up_threads(max_threads);
  max_threads = 64;
  ramp_up_threads(max_threads);
  max_threads = 128;
  ramp_up_threads(max_threads);
  /*max_threads = 256;
  ramp_up_threads(max_threads); */

  /* Clean up mess */
  pthread_mutex_destroy(&key_lock);

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

