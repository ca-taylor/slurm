#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>

#define SERVER_URI_ENV "PMIX_SERVER_URI"
#define JOBID_ENV "PMIX_ID"


typedef struct {
    unsigned int taskid;
    size_t nbytes;
} cli_msg_hdr_t;

int connect_to_server_usock(char *path)
{
  static struct sockaddr_un sa;
    int ret;

  if( strlen(path) >= sizeof(sa.sun_path) ){
    perror("The size of UNIX sockety path is greater than possible");
    return -1;
  }

  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if( fd < 0 ){
    perror("Cannot create socket");
  }

  memset(&sa, 0, sizeof(sa));
  sa.sun_family = AF_UNIX;
  strcpy(sa.sun_path, path);

  if( ret = connect(fd,(struct sockaddr*) &sa, SUN_LEN(&sa) ) ){
  }
  return fd;
}

int main(int argc, char* argv[])
{
  int jid, sid, lrank, grank, ntasks;
  int i = 1;

  char *path = getenv( JOBID_ENV );
  sscanf(path, "%d.%d.%d", &jid, &sid, &lrank);
  path = getenv( SERVER_URI_ENV );
  printf("%d: uri = %s\n",lrank, path);

  int fd = connect_to_server_usock(path);
  if( fd < 0 ){
    printf("ERROR!!!\n");
    return -1;
  }

  // Allocate memory with the reserve
  void *buf = malloc( sizeof(cli_msg_hdr_t) + sizeof(int)*100 );

  // Greeting
  int size = sizeof(cli_msg_hdr_t) + sizeof(int);
  cli_msg_hdr_t *hdr = (cli_msg_hdr_t *)buf;
  int *payload = (int*)(hdr + 1);
  hdr->taskid = lrank;
  hdr->nbytes = 4;
  *((int*)((char*)buf + sizeof(cli_msg_hdr_t))) = lrank;
  write(fd, buf, size);
// Read the answer and get our global rank and task number
  read(fd,buf, sizeof(cli_msg_hdr_t));
  read(fd, (void*)(hdr+1),hdr->nbytes);
  grank = payload[0];
  ntasks = payload[1];

  // Send collective data
  hdr->nbytes = 10*sizeof(int);
  hdr->taskid = lrank;
  for(i=0;i<10;i++){
    payload[i] = grank + i;
  }
  size = sizeof(cli_msg_hdr_t) + hdr->nbytes;
  write(fd, buf, size);

  // Read fence confirmation
  size = sizeof(cli_msg_hdr_t) + sizeof(int);
  read(fd, buf, size);
  printf("Fence finished: %s\n", (*((int*)(hdr + 1)) == 1) ? "OK" : "FAIL");

  // Ask about all others starting from the right neighbor
  int task_to_ask = (grank + 1) % ntasks;
  char fname[1024];
  sprintf(fname, "pmix_dump.%d", grank);
  FILE *fp = fopen(fname, "w");
  for(i = 0; i < ntasks; i++){
      // Send id of the task
      hdr->nbytes = 4;
      hdr->taskid = lrank;
      payload[0] = task_to_ask;
      size = sizeof(cli_msg_hdr_t) + hdr->nbytes;
      write(fd, buf, size);
      
      // Get response
      read(fd, (void*)hdr, sizeof(cli_msg_hdr_t));
      read(fd, (void*)payload,hdr->nbytes);
      fprintf(fp, "%d: ", task_to_ask);
      int j;
      for(j=0; j < hdr->nbytes/sizeof(int); j++){
        fprintf(fp, "%d ", payload[j]);
      }
      fprintf(fp,"\n");
      task_to_ask++;
      task_to_ask %= ntasks;
  }
  fclose(fp);

  close(fd);
  return 0;
}
