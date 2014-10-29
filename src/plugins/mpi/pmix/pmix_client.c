#include "mpi_pmix.h"
#include "pmix_state.h"
#include "pmix_msg.h"
#include "pmix_db.h"

#define MSGSIZE 100

static bool peer_readable(eio_obj_t *obj);
static int peer_read(eio_obj_t *obj, List objs);
static struct io_operations peer_ops = {
  .readable    =  &peer_readable,
  .handle_read =  &peer_read,
};

// Unblocking message processing
typedef struct {
    uint32_t taskid;
    size_t nbytes;
} cli_msg_hdr_t;

static uint32_t payload_size(void *buf)
{
  cli_msg_hdr_t *ptr = (cli_msg_hdr_t*)buf;
  return ptr->nbytes;
}


void pmix_client_request(eio_handle_t *handle, int fd)
{
  cli_msg_hdr_t hdr;
  uint32_t offset = 0;
  eio_obj_t *obj;

  PMIX_DEBUG("Request from fd = %d", fd);
  // fd was just accept()'ed and is blocking for now.
  // TODO/FIXME: implement this in nonblocking fashion
  pmix_msgengine_first_header(fd, &hdr, &offset, sizeof(hdr) );
  xassert( offset == sizeof(hdr));

  uint32_t taskid = hdr.taskid;
  if( pmix_state_cli_connected(taskid,fd) ){
    PMIX_DEBUG("Bad connection, taskid = %d, fd = %d", taskid, fd);
    close(fd);
    return;
  }
  PMIX_DEBUG("Received ")
  // Setup message engine. Push the header we just received to
  // ensure integrity of msgengine
  pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);
  pmix_msgengine_init(me,sizeof(hdr),payload_size);
  pmix_msgengine_add_hdr(me, &hdr);

  obj = eio_obj_create(fd, &peer_ops, (void*)(long)taskid);
  eio_new_obj(handle, obj);
  pmix_state_cli_set_io(taskid, obj);
}

static bool peer_readable(eio_obj_t *obj)
{
  PMIX_DEBUG("fd = %d", obj->fd);
  if (obj->shutdown == true) {
    if (obj->fd != -1) {
      close(obj->fd);
      obj->fd = -1;
    }
    PMIX_DEBUG("    false, shutdown");
    return false;
  }
  return true;
}

static int peer_read(eio_obj_t *obj, List objs)
{
  xassert( !pmix_info_is_srun() );

  PMIX_DEBUG("fd = %d", obj->fd);

  uint32_t taskid = (int)(long)(obj->arg);

  pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);

  // Read and process all received messages
  while( 1 ){
    pmix_msgengine_rcvd(obj->fd, me);
    if( pmix_msgengine_failed(me) ){
      PMIX_ERROR("Client connection with task %d failed", taskid);
      obj->shutdown = true;
      pmix_state_cli_failed_set(taskid);
      // TODO: we need to remove this connection from eio
      // Can we just:
      // 1. find the node in objs that corresponds to obj
      // 2. list_node_destroy (objs, node)
      return 0;
    }
    if( pmix_msgengine_ready(me) ){
      cli_msg_hdr_t hdr;
      uint32_t size;
      void *msg = pmix_msgengine_extract(me, &hdr, &size);
      xassert( hdr.taskid == taskid );
      if( size > 4 ){
        PMIX_DEBUG("Big message!");
      }
      pmix_db_add_blob(taskid, msg);
      xfree(msg);
    }else{
      // No more complete messages
      break;
    }
  }
  return 0;
}

