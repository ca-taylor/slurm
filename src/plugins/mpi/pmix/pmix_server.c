#include "mpi_pmix.h"


#define MSGSIZE 100

void pmix_server_request(eio_handle_t *h, int fd)
{
  PMIX_DEBUG("Request from fd = %d", fd);
  char buf[1024];
  int i = 0, cnt = 0;
  while( i < MSGSIZE ){
    cnt = read(fd, buf + i, MSGSIZE - i);
    if( cnt < 0 ){
      PMIX_ERROR("Reading server request");
      break;
    }
    i += cnt;
  }

  PMIX_DEBUG("Received %s", buf);
}
