#ifndef STATE_H
#define STATE_H

#include "pmix_common.h"
#include "pmix_msg.h"

typedef struct {
  uint32_t task_id;
  int fd;
  eio_obj_t *io_obj;
  pmix_msgengine_t mstate;
} client_state_t;

typedef enum { PMIX_COLL_INIT, PMIX_COLL_LOCAL, PMIX_COLL_SRV, PMIX_COLL_FAIL } pmix_coll_state_t;

typedef struct {
  pmix_coll_state_t rcvd_state;
  uint32_t local_joined;
  uint32_t remote
} collective_state_t;

typedef struct {
#ifndef NDEBUG
#       define PMIX_STATE_MAGIC 0xdeadbeef
  int  magic;
#endif
  uint32_t cli_size;
  client_state_t *cli_state;
  collective_state_t coll;
} pmix_state_t;

extern pmix_state_t pmix_state;

void pmix_state_init();

inline static void pmix_state_sanity_check()
{
  xassert( pmix_state.magic == PMIX_STATE_MAGIC );
}

inline static void pmix_state_cli_sanity_check(uint32_t taskid)
{
  pmix_state_sanity_check();
  xassert( taskid < pmix_state.cli_size);
  xassert( pmix_state.cli_state[taskid].fd >= 0 );
}

inline static int pmix_state_cli_connected(int taskid, int fd)
{
  pmix_state_sanity_check();
  if( !( taskid < pmix_state.cli_size ) ){
    return SLURM_ERROR;
  }
  client_state_t *cli = &pmix_state.cli_state[taskid];
  if( cli->fd >= 0 ){
    // We already have this task connected. Shouldn't happen.
    // FIXME: should we ignore new or old connection? Ignore new by now, discuss with Ralph.
    return SLURM_ERROR;
  }
  cli->task_id = taskid;
  cli->fd = fd;
  cli->io_obj = NULL;
  return SLURM_SUCCESS;
}

inline static pmix_msgengine_t *pmix_state_cli_msghandler(int taskid)
{
  pmix_state_cli_sanity_check(taskid);
  return &pmix_state.cli_state[taskid].mstate;
}

inline static void pmix_state_cli_set_io(int taskid, eio_obj_t *obj)
{
  pmix_state_cli_sanity_check(taskid);

  // We also check that io_obj wasn't initialized yet and we don't mem leak
  xassert( pmix_state.cli_state[taskid].io_obj == NULL );
  pmix_state.cli_state[taskid].io_obj = obj;
}

inline static bool pmix_state_cli_failed(uint32_t taskid)
{
  pmix_state_cli_sanity_check(taskid);
  client_state_t *cli = &pmix_state.cli_state[taskid];
  return pmix_nbmsg_finalized(&cli->mstate);
}

inline static void pmix_state_cli_finalized_set(uint32_t taskid)
{
  pmix_state_cli_sanity_check(taskid);
  client_state_t *cli = &pmix_state.cli_state[taskid];
  // FIXME: do we need to close fd or free io_obj?
  // I assume that this will be done by eio when shutdown flag is on.
  cli->fd = -1;
  cli->io_obj = NULL;
}


#endif // STATE_H
