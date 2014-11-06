#include <string.h>
#include "pmix_common.h"
#include "pmix_debug.h"
#include "pmix_info.h"

// Client communication
static char *_cli_addr = NULL;
static int _cli_fd = -1;

// Server communication
static char *_server_addr = NULL;
static int _server_fd = -1;

struct pmix_jobinfo_t _pmix_job_info  = { 0 };

// Collective tree description
char *_pmix_this_host = NULL;
char *_pmix_nodes_list = NULL;
int _pmix_child_num = -1;
int *_pmix_child_list = NULL;
parent_type_t _pmix_parent_type = PMIX_PARENT_NONE;
char *_pmix_parent_host = NULL;
slurm_addr_t *_pmix_parent_addr = NULL;
uint16_t _pmix_parent_port = -1;


// Client contact information
void pmix_info_cli_contacts_set(char *path, int fd)
{
  int size = strlen(path);
  _cli_addr = xmalloc(size + 1);
  strcpy(_cli_addr,path);
  _cli_fd = fd;
}

const char *pmix_info_cli_addr()
{
  // Check that client address was initialized
  xassert( _cli_addr != NULL );
  return _cli_addr;
}


int pmix_info_cli_fd()
{
  // Check that client fd was created
  xassert( _cli_fd >= 0  );
  return _cli_fd;
}

// slurmd contact information
void pmix_info_server_contacts_set(char *path, int fd)
{
  if( path != NULL ){
    int size = strlen(path);
    _server_addr = xmalloc(size + 1);
    strcpy(_server_addr,path);
  } else {
    _server_addr = NULL;
  }
  _server_fd = fd;
}

const char *pmix_info_srv_addr()
{
  // Check that Server address was initialized
  xassert( _server_addr != NULL );
  return _server_addr;
}

int pmix_info_srv_fd()
{
  // Check that Server fd was created
  xassert( _server_fd >= 0  );
  return _server_fd;
}

// Job information
void pmix_info_job_set_srun(const mpi_plugin_client_info_t *job)
{
  int i;

  memset(&_pmix_job_info, 0, sizeof(_pmix_job_info));
#ifndef NDEBUG
  _pmix_job_info.magic = PMIX_INFO_MAGIC;
#endif

  _pmix_nodes_list = xstrdup(job->step_layout->node_list);
  // This node info
  _pmix_job_info.jobid      = job->jobid;
  _pmix_job_info.stepid     = job->stepid;
  _pmix_job_info.node_id    = -1; /* srun sign */
  _pmix_job_info.node_tasks =  0; /* srun doesn't manage any tasks */
  _pmix_job_info.ntasks     = job->step_layout->task_cnt;
  _pmix_job_info.nnodes     = job->step_layout->node_cnt;
  _pmix_job_info.task_cnts  = xmalloc( sizeof(*_pmix_job_info.task_cnts) * _pmix_job_info.nnodes);
  for(i = 0; i < _pmix_job_info.nnodes; i++){
    _pmix_job_info.task_cnts[i] = job->step_layout->tasks[i];
  }
  _pmix_job_info.task_dist = job->step_layout->task_dist;
  // TODO: we need to extract global task mapping!
  // Check PMI2's static char *_get_proc_mapping(const mpi_plugin_client_info_t *job)
  // for possible way to go.
}

void pmix_info_job_set(const stepd_step_rec_t *job)
{
  int i;
#ifndef NDEBUG
  _pmix_job_info.magic = PMIX_INFO_MAGIC;
#endif

  // This node info
  _pmix_job_info.jobid      = job->jobid;
  _pmix_job_info.stepid     = job->stepid;
  _pmix_job_info.node_id    = job->nodeid;
  _pmix_job_info.node_tasks = job->node_tasks;

  // Global info
  _pmix_job_info.ntasks     = job->ntasks;
  _pmix_job_info.nnodes     = job->nnodes;
  _pmix_job_info.task_cnts  = xmalloc( sizeof(*_pmix_job_info.task_cnts) * _pmix_job_info.nnodes);
  for(i = 0; i < _pmix_job_info.nnodes; i++){
    _pmix_job_info.task_cnts[i] = job->task_cnts[i];
  }

  _pmix_job_info.gtids = xmalloc(_pmix_job_info.node_tasks * sizeof(uint32_t));
  for (i = 0; i < job->node_tasks; i ++) {
    _pmix_job_info.gtids[i] = job->task[i]->gtid;
  }

  // TODO: we need to extract global task mapping!
  // Check PMI2's static char *_get_proc_mapping(const mpi_plugin_client_info_t *job)
  // for possible way to go.
}

// Data related to Collective tree

int pmix_info_nodes_list_set(char ***env)
{
  if( pmix_info_is_srun() ){
    // we already set this value in pmix_info_job_set_srun
    return SLURM_SUCCESS;
  }

  char *p = getenvp(*env, PMIX_STEP_NODES_ENV);
  if (!p) {
    PMIX_ERROR("Environment variable %s not found", PMIX_STEP_NODES_ENV);
    return SLURM_ERROR;
  }
  _pmix_nodes_list = xstrdup(p);
  return SLURM_SUCCESS;
}

int pmix_info_nodes_env_set(char *this_host, int *childs, int child_cnt)
{
  xassert( child_cnt >= 0 );
  xassert( this_host );
  xassert( childs != NULL );
  _pmix_this_host = this_host;
  _pmix_child_num = child_cnt;
  _pmix_child_list = childs;
  return SLURM_SUCCESS;
}

int  pmix_info_parent_set_root(int child_num)
{
  _pmix_parent_type = PMIX_PARENT_ROOT;

  return SLURM_SUCCESS;
}

int  pmix_info_parent_set_srun(char *phost, uint16_t port)
{
  _pmix_parent_type = PMIX_PARENT_SRUN;
  _pmix_parent_addr = xmalloc(sizeof(slurm_addr_t));
  slurm_set_addr(_pmix_parent_addr, port, phost);
  return SLURM_SUCCESS;
}

int pmix_info_parent_set_stepd(char *phost)
{
  _pmix_parent_type = PMIX_PARENT_STEPD;
  _pmix_parent_host = phost;
  return SLURM_SUCCESS;
}

static eio_handle_t *_io_handle = NULL;

void pmix_info_io_set(eio_handle_t *h)
{
  _io_handle = h;
}

eio_handle_t *pmix_info_io()
{
  xassert( _io_handle != NULL );
  return _io_handle;
}

char *pmix_info_nth_child_name(int idx)
{
  hostlist_t hl = hostlist_create(pmix_info_step_hosts());
  int n = pmix_info_nth_child(idx);
  char *p = hostlist_nth(hl, n);
  hostlist_destroy(hl);
  return p;
}

char *pmix_info_nth_host_name(int n)
{
  hostlist_t hl = hostlist_create(pmix_info_step_hosts());
  char *p = hostlist_nth(hl, n);
  hostlist_destroy(hl);
  return p;
}
