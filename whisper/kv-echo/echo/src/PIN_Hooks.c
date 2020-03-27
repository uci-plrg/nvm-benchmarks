/* Katelin Bailey
 * April 2012
 *
 *
 * Hooks for pin memory trace
 */


#include <stdlib.h>
#include <stdio.h>
#include "PIN_Hooks.h"



/* Place this function at the point in your code where you 
 *  transition from worker to master. 
 * May be called once at the beginning of instrumention,
 *  after which it must by preceded by a call to
 *  MASTER2WORKER
 */
__attribute__((noinline)) void WORKER2MASTER(void){
  return;
}

/* Place this function at the point in your code where you 
 *  transition from master to worker. 
 * Must be preceded by a call to WORKER2MASTER 
 */
__attribute__((noinline)) void MASTER2WORKER(void){
  return;
}

/* Place this function at the point in your code where you
 *  which to end an isolated count operation 
 *  (e.g after a microbenchmark). Call with an id number that
 *  will precede the new stats in the trace.out. 
 */
 __attribute__((noinline)) void CLEAR_COUNTS_SET_ID(int id){
  return;
}

/* Used to turn PIN instrumentation on. 
 * Need not be called at the beginning; pin is automatically on */
__attribute__((noinline)) void INSTRUMENT_ON(void){
  return;
}

/* Used to turn PIN instrumentation (temporarily) off
 * Need not be called at the end; pin is automatically terminated */
__attribute__((noinline)) void INSTRUMENT_OFF(void){
  return;
}
