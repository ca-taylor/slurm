#ifndef STATE_H
#define STATE_H

#include "pmix_common.h"
#include "pmix_msg.h"

typedef enum { PMIX_CLI_UNCONNECTED, PMIX_CLI_ACK, PMIX_CLI_OPERATE, PMIX_CLI_COLL, PMIX_CLI_FINALIZED} pmix_cli_state_t;

typedef struct {
  pmix_cli_state_t state;
  uint32_t task_id;
  int fd;
  pmix_msgengine_t mstate;
} client_state_t;

typedef enum { PMIX_COLL_SYNC, PMIX_COLL_GATHER, PMIX_COLL_FORWARD } pmix_coll_state_t;

typedef struct {
  pmix_coll_state_t state;
  uint32_t local_joined;
  uint8_t *local_contrib;
  uint32_t nodes_joined;
  uint8_t *nodes_contrib;
} collective_state_t;

typedef struct {
#ifndef NDEBUG
#       define PMIX_STATE_MAGIC 0xdeadbeef
  int  magic;
#endif
  uint32_t cli_size;
  client_state_t *cli_state;
  collective_state_t coll;
  eio_handle_t *cli_handle, *srv_handle;
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
  // TODO: will need to implement additional step - ACK
  cli->state = PMIX_CLI_OPERATE;
  cli->task_id = taskid;
  cli->fd = fd;
  return SLURM_SUCCESS;
}

inline static pmix_msgengine_t *pmix_state_cli_msghandler(int taskid)
{
  pmix_state_cli_sanity_check(taskid);
  return &pmix_state.cli_state[taskid].mstate;
}

inline static bool pmix_state_cli_finalized(uint32_t taskid)
{
  pmix_state_cli_sanity_check(taskid);
  client_state_t *cli = &pmix_state.cli_state[taskid];
  return (cli->state == PMIX_CLI_FINALIZED);
}

inline static void pmix_state_cli_finalized_set(uint32_t taskid)
{
  pmix_state_cli_sanity_check(taskid);
  client_state_t *cli = &pmix_state.cli_state[taskid];
  // FIXME: do we need to close fd or free io_obj?
  // I assume that this will be done by eio when shutdown flag is on.
  cli->fd = -1;
  cli->state = PMIX_CLI_FINALIZED;
}

bool pmix_state_node_contrib_ok(int idx);
bool pmix_state_task_contrib_ok(int idx);
bool pmix_state_coll_local_ok();
bool pmix_state_coll_forwad();
bool pmix_state_node_contrib_cancel(int idx);
bool pmix_state_task_contrib_cancel(int idx);
inline static void pmix_state_task_coll_finish(uint32_t taskid)
{
  pmix_state_cli_sanity_check(taskid);
  client_state_t *cli = &pmix_state.cli_state[taskid];
  xassert( cli->state == PMIX_CLI_COLL );
  cli->state = PMIX_CLI_OPERATE;
}


#endif // STATE_H
