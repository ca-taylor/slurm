#include <string.h>
#include "mpi_pmix.h"

// Client communication
static char *_cli_addr = NULL;
static int _cli_fd = -1;

// Server communication
static char *_server_addr = NULL;
static int _server_fd = -1;

// Agent location
static int _in_stepd = -1;

struct pmix_jobinfo_t __pmix_job_info  = { 0 };

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
  int size = strlen(path);
  _server_addr = xmalloc(size + 1);
  strcpy(_server_addr,path);
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

// Agent location

void pmix_info_is_stepd_set()
{
  _in_stepd = 1;
}

void pmix_info_is_srun_set()
{
  _in_stepd = 0;
}

int pmix_info_is_stepd()
{
  xassert( _in_stepd >= 0 );
  return (_in_stepd == 1);
}


int pmix_info_is_srun()
{
  xassert( _in_stepd >= 0 );
  return (_in_stepd == 0);
}

// Job information

void pmix_info_job_set(const stepd_step_rec_t *job)
{
  int i;
#ifndef NDEBUG
  __pmix_job_info.magic = PMIX_INFO_MAGIC;
#endif

  // This node info
  __pmix_job_info.jobid      = job->jobid;
  __pmix_job_info.stepid     = job->stepid;
  __pmix_job_info.node_id    = job->nodeid;
  __pmix_job_info.node_tasks = job->node_tasks;

  // Global info
  __pmix_job_info.ntasks     = job->ntasks;
  __pmix_job_info.nnodes     = job->nnodes;
  __pmix_job_info.task_cnts  = xmalloc( sizeof(*__pmix_job_info.task_cnts) * __pmix_job_info.nnodes);
  for(i = 0; i < __pmix_job_info.nnodes; i++){
    __pmix_job_info.task_cnts[i] = job->task_cnts[i];
  }

  __pmix_job_info.gtids = xmalloc(__pmix_job_info.node_tasks * sizeof(uint32_t));
  for (i = 0; i < job->node_tasks; i ++) {
    __pmix_job_info.gtids[i] = job->task[i]->gtid;
  }

  // TODO: we need to extract global task mapping!
  // Check PMI2's static char *_get_proc_mapping(const mpi_plugin_client_info_t *job)
  // for possible way to go.
}

