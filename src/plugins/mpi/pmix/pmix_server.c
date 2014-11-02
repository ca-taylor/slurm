#include "pmix_common.h"
#include "pmix_info.h"
#include "pmix_coll.h"
#include "pmix_debug.h"
#include "pmix_msg.h"
#include "pmix_client.h"

int pmix_stepd_init(const stepd_step_rec_t *job, char ***env)
{
  char path[MAX_USOCK_PATH];
  int fd;

  // Create UNIX socket for slurmd communication
  sprintf(path, PMIX_STEPD_ADDR_FMT, job->jobid, job->stepid );
  if( (fd = pmix_comm_srvsock_create(path)) < 0 ){
    return SLURM_ERROR;
  }
  pmix_info_server_contacts_set(path, fd);

  // Create UNIX socket for client communication
  sprintf(path, PMIX_CLI_ADDR_FMT, job->jobid, job->stepid );
  if( (fd = pmix_comm_srvsock_create(path)) < 0 ){
    close( pmix_info_srv_fd() );
    return SLURM_ERROR;
  }
  pmix_info_cli_contacts_set(path, fd);
  pmix_info_job_set(job);
  pmix_coll_init(env);
  return SLURM_SUCCESS;
}

int pmix_srun_init(const mpi_plugin_client_info_t *job, char ***env)
{
  char path[MAX_USOCK_PATH];
  int fd;
  uint16_t port;
  if (net_stream_listen(&fd, &port) < 0) {
    PMIX_ERROR("Failed to create tree socket");
    return SLURM_ERROR;
  }
  sprintf(path, PMIX_STEPD_ADDR_FMT, job->jobid, job->stepid );
  pmix_info_server_contacts_set(path, fd);
  PMIX_DEBUG("srun pmi port: %hu", port);
  env_array_overwrite_fmt(env, PMIX_SRUN_PORT_ENV, "%hu", port);
  pmix_info_job_set_srun(job);
  pmix_coll_init(env);
  return SLURM_SUCCESS;
}

static bool serv_readable(eio_obj_t *obj);
static int serv_read(eio_obj_t *obj, List objs);
static struct io_operations peer_ops = {
  .readable     = serv_readable,
  .handle_read  = serv_read
};

// Unblocking message processing

enum { PMIX_FENCE, PMIX_FENCE_RESP };

#define PMIX_SERVER_MSG_MAGIC 0xdeadbeef
typedef struct {
  uint32_t magic;
  uint32_t nodeid;
  uint32_t paysize;
  int cmd;
} srv_sendmsg_hdr_t;

typedef struct {
  uint32_t size; // Fas to be first (appended by SLURM API)
  srv_sendmsg_hdr_t send_hdr;
} srv_recvmsg_hdr_t;


char *pmix_server_alloc_msg(uint32_t size, char **payload)
{
  char *msg = xmalloc(sizeof(srv_sendmsg_hdr_t) + size);
  srv_sendmsg_hdr_t *hdr = (srv_sendmsg_hdr_t*)msg;
  hdr->magic = PMIX_SERVER_MSG_MAGIC;
  hdr->nodeid = pmix_info_nodeid();
  hdr->paysize = size;
  *payload = (char*)(hdr + 1);
  return msg;
}

void pmix_server_msg_set_fence(char *msg)
{
  xassert(msg != NULL);
  srv_sendmsg_hdr_t *hdr = (srv_sendmsg_hdr_t*)msg;
  hdr->cmd = PMIX_FENCE;
}

void pmix_server_msg_set_fence_resp(char *msg)
{
  xassert(msg != NULL);
  srv_sendmsg_hdr_t *hdr = (srv_sendmsg_hdr_t*)msg;
  hdr->cmd = PMIX_FENCE_RESP;
}

uint32_t pmix_server_sendmsg_size(char *msg)
{
  xassert(msg != NULL);
  srv_sendmsg_hdr_t *hdr = (srv_sendmsg_hdr_t*)msg;
  return hdr->paysize + sizeof(*hdr);
}

static uint32_t _recv_payload_size(void *buf)
{
  srv_recvmsg_hdr_t *ptr = (srv_recvmsg_hdr_t*)buf;
  srv_sendmsg_hdr_t *hdr = &ptr->send_hdr;
  ptr->size = ntohl(ptr->size);
  xassert( ptr->size == (sizeof(*hdr) + hdr->paysize) );
  xassert( hdr->magic == PMIX_SERVER_MSG_MAGIC );
  return hdr->paysize;
}

void pmix_server_request(int fd)
{
  eio_obj_t *obj;
  PMIX_DEBUG("Request from fd = %d", fd);
  // Set nonblocking
  fd_set_nonblocking(fd);

  pmix_msgengine_t *me = xmalloc( sizeof(pmix_msgengine_t) );
  pmix_nbmsg_init(me,fd, sizeof(srv_recvmsg_hdr_t),_recv_payload_size);
  if( pmix_info_is_stepd() ){
    // We use slurm_forward_data to send message to stepd's
    // SLURM will put user ID there. We need to skip it
    pmix_nbmsg_set_padding(me, sizeof(uint32_t));
  }
  obj = eio_obj_create(fd, &peer_ops, (void*)me);
  eio_new_obj(pmix_info_io(), obj);
}

static bool serv_readable(eio_obj_t *obj)
{
  PMIX_DEBUG("fd = %d", obj->fd);
  if (obj->shutdown == true) {
    if (obj->fd != -1) {
      close(obj->fd);
      pmix_msgengine_t *me = (pmix_msgengine_t*)obj->arg;
      pmix_nbmsg_finalize(me);
      xfree(me);
      obj->fd = -1;
    }
    PMIX_DEBUG("    false, shutdown");
    return false;
  }
  return true;
}

void _process_collective_request(srv_recvmsg_hdr_t *_hdr, void *payload)
{
  srv_sendmsg_hdr_t *hdr = &_hdr->send_hdr;
  switch( hdr->cmd ){
  case PMIX_FENCE:
    pmix_coll_node_contrib(hdr->nodeid, payload, hdr->paysize);
    break;
  case PMIX_FENCE_RESP:
    pmix_coll_update_db(payload, hdr->paysize);
    pmix_client_fence_notify();
    break;
  default:
    PMIX_ERROR("Bad command %d", hdr->cmd);
  }
}

static int serv_read(eio_obj_t *obj, List objs)
{

  PMIX_DEBUG("fd = %d", obj->fd);
  pmix_msgengine_t *me = (pmix_msgengine_t *)obj->arg;

  // Read and process all received messages
  while( 1 ){
    pmix_nbmsg_rcvd(me);
    if( pmix_nbmsg_finalized(me) ){
      PMIX_ERROR("Connection finalized fd = %d", obj->fd);
      obj->shutdown = true;
      return 0;
    }
    if( pmix_nbmsg_rcvd_ready(me) ){
      srv_recvmsg_hdr_t hdr;
      void *msg = pmix_nbmsg_rcvd_extract(me, &hdr);
      _process_collective_request(&hdr, msg);
    }else{
      // No more complete messages
      break;
    }
  }
  return 0;
}


