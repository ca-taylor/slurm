#include "mpi_pmix.h"
#include "pmix_state.h"
#include "pmix_msg.h"
#include "pmix_db.h"

#define MSGSIZE 100

static bool peer_readable(eio_obj_t *obj);
static int peer_read(eio_obj_t *obj, List objs);
static bool peer_writable(eio_obj_t *obj);
static int peer_write(eio_obj_t *obj, List objs);
static struct io_operations peer_ops = {
  .readable     = peer_readable,
  .handle_read  = peer_read,
  .writable     = peer_writable,
  .handle_write = peer_write
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

  //    {
  //        int delay = 1;
  //        while( delay ){
  //      sleep(1);
  //        }
  //    }

  PMIX_DEBUG("Request from fd = %d", fd);

  // fd was just accept()'ed and is blocking for now.
  // TODO/FIXME: implement this in nonblocking fashion?
  pmix_nbmsg_first_header(fd, &hdr, &offset, sizeof(hdr) );
  xassert( offset == sizeof(hdr));

  // Set nonblocking
  fd_set_nonblocking(fd);

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
  pmix_nbmsg_init(me,sizeof(hdr),payload_size);
  pmix_nbmsg_add_hdr(me, &hdr);

  obj = eio_obj_create(fd, &peer_ops, (void*)(long)taskid);
  eio_new_obj(handle, obj);
  pmix_state_cli_set_io(taskid, obj);
}

static bool peer_readable(eio_obj_t *obj)
{
  PMIX_DEBUG("fd = %d", obj->fd);
  xassert( !pmix_info_is_srun() );
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

  PMIX_DEBUG("fd = %d", obj->fd);
  uint32_t taskid = (int)(long)(obj->arg);
  pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);

  // Read and process all received messages
  while( 1 ){
    pmix_nbmsg_rcvd(obj->fd, me);
    if( pmix_nbmsg_finalized(me) ){
      PMIX_ERROR("Connection with task %d finalized", taskid);
      obj->shutdown = true;
      pmix_state_cli_finalized_set(taskid);
      // TODO: we need to remove this connection from eio
      // Can we just:
      // 1. find the node in objs that corresponds to obj
      // 2. list_node_destroy (objs, node)
      return 0;
    }
    if( pmix_nbmsg_rcvd_ready(me) ){
      cli_msg_hdr_t hdr;
      uint32_t size;
      void *msg = pmix_nbmsg_rcvd_extract(me, &hdr, &size);
      xassert( hdr.taskid == taskid );
//      if( size > 4 ){
//        PMIX_DEBUG("Big message!");
//      }
//      pmix_db_add_blob(taskid, msg);
      xfree(msg);
      {
        int i = 0;
        for(i=0;i<100;i++){
          void *buf = xmalloc(sizeof(cli_msg_hdr_t) + 100000);
          cli_msg_hdr_t *hdr = (cli_msg_hdr_t*)buf;
          hdr->taskid = 0;
          hdr->nbytes = 10000;
          pmix_nbmsg_send_enqueue(obj->fd,me,buf);
        }
      }
    }else{
      // No more complete messages
      break;
    }
  }
  return 0;
}


static bool peer_writable(eio_obj_t *obj)
{
  xassert( !pmix_info_is_srun() );
  PMIX_DEBUG("fd = %d", obj->fd);
  if (obj->shutdown == true) {
    if (obj->fd != -1) {
      close(obj->fd);
      obj->fd = -1;
    }
    PMIX_DEBUG("    false, shutdown");
    return false;
  }
  uint32_t taskid = (int)(long)(obj->arg);
  pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);
  //pmix_comm_fd_write_ready(obj->fd);
  if( pmix_nbmsg_send_pending(me) )
    return true;
  return false;
}

static int peer_write(eio_obj_t *obj, List objs)
{
  xassert( !pmix_info_is_srun() );

  PMIX_DEBUG("fd = %d", obj->fd);

  uint32_t taskid = (int)(long)(obj->arg);

  pmix_msgengine_t *me = pmix_state_cli_msghandler(taskid);
  pmix_nbmsg_send_progress(obj->fd, me);

  return 0;
}
