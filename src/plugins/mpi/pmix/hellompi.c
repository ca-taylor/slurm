#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>


//--------------------------------------------- 8< ------------------------------------------------------

#define SERVER_URI_ENV "PMIX_SERVER_URI"
#define JOBID_ENV "PMIX_ID"

#define PMIX_VERSION "1.0"


// From PMIx server
/* define some commands */
#define PMIX_ABORT_CMD        1
#define PMIX_FENCE_CMD        2
#define PMIX_FENCENB_CMD      3
#define PMIX_PUT_CMD          4
#define PMIX_GET_CMD          5
#define PMIX_GETNB_CMD        6
#define PMIX_FINALIZE_CMD     7
#define PMIX_GETATTR_CMD      8


/* define some message types */
#define PMIX_USOCK_IDENT  1
#define PMIX_USOCK_USER   2

typedef unsigned int uint32_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned long long uint64_t;

typedef struct {
	char hname[32];
	char tmpdir[256];
	char jobid[32];
	uint32_t appnum;
	uint32_t rank;
	uint32_t grank;
	uint32_t apprank;
	uint32_t offset;
	uint16_t lrank;
	uint16_t nrank;
	uint64_t lldr;
	uint32_t aldr;
	uint32_t usize;
	uint32_t jsize;
	uint32_t lsize;
	uint32_t nsize;
	uint32_t msize;
} job_attr_t;

job_attr_t jattr;

#define PMIX_CLIENT_HDR_MAGIC 0xdeadbeef
typedef struct {
	uint32_t magic;
	uint32_t taskid;
	uint8_t type;
	uint32_t tag;
	size_t nbytes;
} message_header_t;

static void *_allocate_msg_to_task(uint32_t taskid, uint8_t type, uint32_t tag, uint32_t size, void **payload)
{
	int msize = size + sizeof(message_header_t);
	message_header_t *msg = (message_header_t*)malloc( msize );
	msg->magic = PMIX_CLIENT_HDR_MAGIC;
	msg->nbytes = size;
	msg->taskid = taskid;
	msg->tag = tag;
	msg->type = type;
	*payload = (void*)(msg + 1);
	return msg;
}

#define _new_msg_connecting(id, size, pay) _allocate_msg_to_task(id, PMIX_USOCK_IDENT, 0, size, pay)
#define _new_msg(id, size, pay) _allocate_msg_to_task(id, PMIX_USOCK_USER, 0, size, pay)
#define _new_msg_tag(id, tag, size, pay) _allocate_msg_to_task(id, PMIX_USOCK_USER, tag, size, pay)

//--------------------------------------------- 8< ------------------------------------------------------

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

int PMI_fd = -1;
int blob_size = 10*1024;
char *blob = NULL;
char **proc_blobs = NULL;

static int pmix_recv_bytes(int fd, char *buf, int size)
{
	int count = 0;
	while( count < size ){
		int cnt = read(fd, buf + count, size - count);
		if( cnt < 0 ){
			perror("read");
			return -1;
		}
		count += cnt;
	}
	return 0;
}

int PMIx_Init(int *rank, int *jsize, int *appnum)
{
	void *msg, *payload;
	int jid, sid, lrank;
	int buf[256];
	char *ptr = (char*)buf;
	message_header_t hdr;

	char *p = getenv( JOBID_ENV );
	sscanf(p, "%d.%d.%d", &jid, &sid, &lrank);
	char *path = getenv( SERVER_URI_ENV );
	printf("%d: uri = %s\n",lrank, path);

	PMI_fd = connect_to_server_usock(path);
	if( PMI_fd < 0 ){
		perror("connect_to_server_usock()\n");
		return -1;
	}

	// Handshake
	int size = sizeof(PMIX_VERSION)+1;
	msg = _new_msg_connecting(lrank, size ,&payload);
	memcpy((char*)payload,PMIX_VERSION, size);
	write(PMI_fd,msg, size + sizeof(message_header_t));
	free(msg);

	if( pmix_recv_bytes(PMI_fd, (char*)&hdr, sizeof(message_header_t)) ){
		printf("PMIx_Init: error reading handshake ack header\n");
		return -1;
	}
	if( pmix_recv_bytes(PMI_fd,ptr,hdr.nbytes) ) {
		printf("PMIx_Init: error reading handshake ack body\n");
		return -1;
	}
	if( strcmp(ptr, PMIX_VERSION) ){
		printf("PMIx_Init: error handshake PMIx  version mismatch\n");
		return -1;
	}

	// get job attributes
	size = 4;
	msg = _new_msg(lrank, size ,&payload);
	*(int*)payload = PMIX_GETATTR_CMD;
	write(PMI_fd,msg, size + sizeof(message_header_t));
	free(msg);

	if( pmix_recv_bytes(PMI_fd, (char*)&hdr, sizeof(message_header_t)) ){
		printf("PMIx_Init: error reading get_attr header\n");
		return -1;
	}
	if( hdr.nbytes != sizeof(jattr) ){
		printf("PMIx_Init: error get_attr body size mismatch\n");
		return -1;
	}
	if( pmix_recv_bytes(PMI_fd,(char*)&jattr,hdr.nbytes) ){
		printf("PMIx_Init: error reading get_attr body\n");
		return -1;
	}

	*rank = jattr.grank;
	*jsize = jattr.jsize;
	*appnum = jattr.appnum;
	return 0;
}

void print_jattr()
{
	printf("Job ID %s\n", jattr.jobid);
	printf("\thost: %s\n", jattr.hname);
	printf("\ttmpdir = %s\n",jattr.tmpdir);
	printf("\tlrank = %u\n", jattr.lrank);
	printf("\tnrank = %u\n", jattr.nrank);
	printf("\tjob size = %u\n", jattr.jsize);
	printf("\tlocal size = %u\n", jattr.lsize);
	printf("\tuniverse size = %u\n",jattr.usize);
}

int PMIx_Put(char *key, char *val)
{
	if( blob == NULL ){
		blob = malloc(blob_size);
		blob[0] = '\0';
	}
	sprintf(blob,"%s%s=%s,",blob,key,val);
}

int PMIx_Fence()
{
	int size = strlen(blob)+1;
	int ret;
	void *msg, *payload;
	message_header_t hdr;

	// get job attributes
	msg = _new_msg(jattr.lrank, size + 4 ,&payload);
	*(int*)payload = PMIX_FENCE_CMD;
	memcpy( (int*)payload + 1, blob, size);
	size += 4;
	write(PMI_fd,msg, size + sizeof(message_header_t));
	free(msg);

	if( pmix_recv_bytes(PMI_fd, (char*)&hdr, sizeof(message_header_t)) ){
		printf("PMIx_Init: error reading fence response header\n");
		return -1;
	}
	if( hdr.nbytes != sizeof(ret) ){
		printf("PMIx_Init: error fence body size mismatch\n");
		return -1;
	}
	if( pmix_recv_bytes(PMI_fd,(char*)&ret,hdr.nbytes) ){
		printf("PMIx_Init: error reading fence response body\n");
		return -1;
	}

	proc_blobs = malloc(jattr.jsize*sizeof(char*));
	memset(proc_blobs,0,jattr.jsize*sizeof(char*));

	return ret;
}

int PMIx_Get(int rank, char *key, char **ptr)
{
	void *msg, *payload;
	message_header_t hdr;
	int size;

	// get job attributes
	if( proc_blobs[rank] == NULL ){
		size = 2*sizeof(int);
		msg = _new_msg(jattr.lrank, size ,&payload);
		*(int*)payload = PMIX_GET_CMD;
		*((int*)payload+1) = rank;
		write(PMI_fd,msg, size + sizeof(message_header_t));
		free(msg);

		if( pmix_recv_bytes(PMI_fd, (char*)&hdr, sizeof(message_header_t)) ){
			printf("PMIx_Get: error reading get response header\n");
			return -1;
		}

		proc_blobs[rank] = malloc(hdr.nbytes);

		if( pmix_recv_bytes(PMI_fd,(char*)proc_blobs[rank], hdr.nbytes) ){
			printf("PMIx_Get: error reading get response body\n");
			return -1;
		}
	}
	char *p = strstr(proc_blobs[rank],key);
	if( p == NULL ){
		printf("PMIx_Get: key %s wasn't found\n", key);
		return -1;
	}
	p = strchr(p, '=');
	if( p == NULL ){
		printf("PMIx_Get: bad key %s format\n", key);
		return -1;
	}
	p++;
	char *p2 = strchr(p,',');
	size = p2 - p;
	*ptr = malloc((p2-p) + 1);
	memcpy(*ptr,p,size);
	(*ptr)[size] = '\0';
	return 0;
}

int main(int argc, char* argv[])
{
	int rank, size, appnum;
	int i = 1;
	char buf[64];



	if( PMIx_Init(&rank, &size, &appnum) ){
		printf("PMIx_Init error\n");
		exit(0);
	}

	fclose(stdout);
	sprintf(buf,"output.%d", rank);
	stdout = fopen(buf,"w");


	printf("rank=%d, size = %d, appnum = %d\n",rank, size, appnum);
	print_jattr();

	// 1st key
	sprintf(buf,"I am rank %d", rank);
	PMIx_Put("the-rank",buf);

	// 2nd key
	sprintf(buf,"My size is %d", size);
	PMIx_Put("the-size",buf);

	// 3nd key
	sprintf(buf,"My appnum is %d", appnum);
	PMIx_Put("the-appnum",buf);

	printf("Fence result: %d\n", PMIx_Fence());


//	{
//		int delay = 1;
//		while( delay ){
//		sleep(1);
//		}
//	}

	for(i=0;i<size;i++){
		char *ptr;
		char buf[1024] = "";
		PMIx_Get(i, "the-rank",&ptr);
		sprintf(buf,"%s%d: the-rank = %s, ", buf, i, ptr);
		free(ptr);
		PMIx_Get(i, "the-size",&ptr);
		sprintf(buf, "%sthe-size = %s, ",buf, ptr);
		free(ptr);
		PMIx_Get(i, "the-appnum",&ptr);
		sprintf(buf, "%sthe-appnum = %s\n", buf, ptr);
		free(ptr);
		printf("%s",buf);
	}
	return 0;
}

/*

  // Allocate memory with the reserve
  void *buf = malloc( sizeof(message_header_t) + sizeof(int)*100 );

  // Greeting
  int size = sizeof(message_header_t) + sizeof(int);
  message_header_t *hdr = (message_header_t *)buf;
  int *payload = (int*)(hdr + 1);
  hdr->taskid = lrank;
  hdr->nbytes = 4;
  *((int*)((char*)buf + sizeof(message_header_t))) = lrank;
  write(fd, buf, size);
// Read the answer and get our global rank and task number
  read(fd,buf, sizeof(message_header_t));
  read(fd, (void*)(hdr+1),hdr->nbytes);
  grank = payload[0];
  ntasks = payload[1];

  // Send collective data
  hdr->nbytes = 10*sizeof(int);
  hdr->taskid = lrank;
  for(i=0;i<10;i++){
	payload[i] = grank + i;
  }
  size = sizeof(message_header_t) + hdr->nbytes;
  write(fd, buf, size);

  // Read fence confirmation
  size = sizeof(message_header_t) + sizeof(int);
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
	  size = sizeof(message_header_t) + hdr->nbytes;
	  write(fd, buf, size);

	  // Get response
	  read(fd, (void*)hdr, sizeof(message_header_t));
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
*/
