#include <pthread.h>
#include <poll.h>
#include <arpa/inet.h>

#include "mpi_pmix.h"
#include "pmix_server.h"
#include "pmix_client.h"
#include "pmix_state.h"
#include "pmix_db.h"

#define MAX_RETRIES 5

static pthread_t pmix_agent_tid = 0;
static eio_handle_t *pmix_io_handle = NULL;

static bool _server_conn_readable(eio_obj_t *obj);
static int  _server_conn_read(eio_obj_t *obj, List objs);

static struct io_operations srv_ops = {
  .readable    = &_server_conn_readable,
  .handle_read = &_server_conn_read,
};


static bool _cli_conn_readable(eio_obj_t *obj);
static int  _cli_conn_read(eio_obj_t *obj, List objs);
/* static bool _task_writable(eio_obj_t *obj); */
/* static int  _task_write(eio_obj_t *obj, List objs); */
static struct io_operations cli_ops = {
  .readable    =  &_cli_conn_readable,
  .handle_read =  &_cli_conn_read,
};


static bool _server_conn_readable(eio_obj_t *obj)
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

static int
_server_conn_read(eio_obj_t *obj, List objs)
{
  int sd;
  struct sockaddr addr;
  struct sockaddr_in *sin;
  socklen_t size = sizeof(addr);
  char buf[INET_ADDRSTRLEN];

  PMIX_DEBUG("fd = %d", obj->fd);

  while (1) {
    /*
     * Return early if fd is not now ready
     */
    if (!pmix_comm_fd_is_ready(obj->fd))
      return 0;

    while ((sd = accept(obj->fd, &addr, &size)) < 0) {
      if (errno == EINTR)
        continue;
      if (errno == EAGAIN)    /* No more connections */
        return 0;
      if ((errno == ECONNABORTED) ||
          (errno == EWOULDBLOCK)) {
        return 0;
      }
      PMIX_ERROR("unable to accept new connection");
      return 0;
    }

    if( pmix_info_is_srun() ){
      sin = (struct sockaddr_in *) &addr;
      inet_ntop(AF_INET, &sin->sin_addr, buf, INET_ADDRSTRLEN);
      PMIX_DEBUG("accepted tree connection: ip=%s sd=%d", buf, sd);
    }

    /* read command from socket and handle it */
    pmix_server_request(pmix_io_handle, sd);
    close(sd);
  }
  return 0;
}

// Client request processing

static bool _cli_conn_readable(eio_obj_t *obj)
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

static int _cli_conn_read(eio_obj_t *obj, List objs)
{
  int fd;

  {
    static int delay = 1;
    while( delay ){
      sleep(1);
    }
  }

  PMIX_DEBUG("fd = %d", obj->fd);

  while (1) {
    /*
     * Return early if fd is not now ready
     */
    if (!pmix_comm_fd_is_ready(obj->fd))
      return 0;

    while ((fd = accept(obj->fd, NULL, 0)) < 0) {
      if (errno == EINTR)
        continue;
      if (errno == EAGAIN)    /* No more connections */
        return 0;
      if ((errno == ECONNABORTED) ||
          (errno == EWOULDBLOCK)) {
        return 0;
      }
      PMIX_ERROR("unable to accept new connection");
      return 0;
    }

    if( pmix_info_is_srun() ){
      PMIX_ERROR("srun shouldn't be directly connected to clients");
      return 0;
    }

    /* read command from socket and handle it */
    pmix_client_request(pmix_io_handle, fd);
  }
  return 0;
}


/*
 * main loop of agent thread
 */
static void *_agent(void * unused)
{

  eio_obj_t *srv_obj, *cli_obj;

  pmix_io_handle = eio_handle_create();

  //fd_set_nonblocking(tree_sock);
  srv_obj = eio_obj_create(pmix_info_srv_fd(), &srv_ops, (void *)(-1));
  eio_new_initial_obj(pmix_io_handle, srv_obj);

  /* for stepd, add the sockets to tasks */
  if (pmix_info_is_stepd()) {
    cli_obj = eio_obj_create(pmix_info_cli_fd(), &cli_ops, (void*)(long)(-1));
    eio_new_initial_obj(pmix_io_handle, cli_obj);
  }

  pmix_state_init();
  pmix_db_init();

  eio_handle_mainloop(pmix_io_handle);

  PMIX_DEBUG("agent thread exit");

  //pmix_state_destroy();
  eio_handle_destroy(pmix_io_handle);
  return NULL;
}


int pmix_agent_start(void)
{
  int retries = 0;
  pthread_attr_t attr;

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
  while ((errno = pthread_create(&pmix_agent_tid, &attr, _agent, NULL))) {
    if (++retries > MAX_RETRIES) {
      error ("mpi/pmi2: pthread_create error %m");
      slurm_attr_destroy(&attr);
      return SLURM_ERROR;
    }
    sleep(1);
  }
  slurm_attr_destroy(&attr);
  PMIX_DEBUG("started agent thread (%lu)", (unsigned long) pmix_agent_tid);

  return SLURM_SUCCESS;
}

void pmix_agent_task_cleanup()
{
  close(pmix_info_cli_fd());
  close(pmix_info_srv_fd());
}
