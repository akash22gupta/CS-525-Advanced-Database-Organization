#include "buffer_mgr.h"

#include <stdio.h>

int 
main (void) 
{
  initStorageManager();
  createPageFile("testbuffer.bin");
  printf("test");
}
