
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>

#include "../masstree/mt_c_wrappers.h"
#include "../kp_macros.h"



int main(int argc, char*argv[]){
  int i;
  pid_t c;
  for(i = 0; i < 15; i++){
    do_forked_run( (int) (*masstree_verify_bindings));
  }
    
  
  return 0;
}
