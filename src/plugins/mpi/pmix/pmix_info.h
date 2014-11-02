#ifndef INFO_H
#define INFO_H

#include "pmix_common.h"

// Job information
struct pmix_jobinfo_t {
#ifndef NDEBUG
#       define PMIX_INFO_MAGIC 0xdeadbeef
  int  magic;
#endif
  uint32_t jobid;       /* Current SLURM job id                      */
  uint32_t stepid;      /* Current step id (or NO_VAL)               */
  uint32_t nnodes;      /* number of nodes in current job            */
  uint32_t ntasks;      /* total number of tasks in current job      */
  uint16_t *task_cnts;  /* Number of tasks on each node in job       */
  int node_id;          /* relative position of this node in job     */
  uint32_t node_tasks;  /* number of tasks on *this* node            */
  uint32_t *gtids;
  uint32_t task_dist;
  // TODO: remove later
  //eio_handle_t  *eio; ??
  // List 	       sruns; ?? /* List of srun_info_t pointers               */
  // List           clients; ?? /* List of struct client_io_info pointers   */
};

typedef enum {PMIX_PARENT_NONE, PMIX_PARENT_ROOT, PMIX_PARENT_SRUN, PMIX_PARENT_STEPD } parent_type_t;
extern struct pmix_jobinfo_t _pmix_job_info;

// Client contact information
void pmix_info_cli_contacts_set(char *path, int fd);
const char *pmix_info_cli_addr();
int pmix_info_cli_fd();

// slurmd contact information
void pmix_info_server_contacts_set(char *path, int fd);
const char *pmix_info_srv_addr();
int pmix_info_srv_fd();

// Agent location
static inline int pmix_info_is_srun()
{
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  return (_pmix_job_info.node_id < 0 );
}

static inline int pmix_info_is_stepd()
{
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  return ( _pmix_job_info.node_id >= 0 );
}

// Job information
void pmix_info_job_set(const stepd_step_rec_t *job);
void pmix_info_job_set_srun(const mpi_plugin_client_info_t *job);

inline static uint32_t pmix_info_jobid(){
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  return _pmix_job_info.jobid;
}

inline static uint32_t pmix_info_stepid(){
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  return _pmix_job_info.stepid;
}

inline static uint32_t pmix_info_nodeid(){
  // xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  // This routine is called from PMIX_DEBUG/ERROR and
  // this CAN happen before initialization. Relax demand to have
  // _pmix_job_info.magic == PMIX_INFO_MAGIC
  return _pmix_job_info.node_id;
}

inline static uint32_t pmix_info_nodes(){
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  return _pmix_job_info.nnodes;
}

inline static uint32_t pmix_info_tasks(){
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  return _pmix_job_info.ntasks;
}

inline static uint32_t pmix_info_node_taskcnt(uint32_t i){
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  xassert( i < _pmix_job_info.nnodes);
  return _pmix_job_info.task_cnts[i];
}


inline static uint32_t pmix_info_ltasks(){
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  return _pmix_job_info.node_tasks;
}

inline static uint32_t pmix_info_task_id(uint32_t i){
  xassert(_pmix_job_info.magic == PMIX_INFO_MAGIC );
  xassert( i < _pmix_job_info.node_tasks );
  return _pmix_job_info.gtids[i];
}

extern parent_type_t _pmix_parent_type;
extern char *_pmix_this_host;
extern char *_pmix_nodes_list;
extern int _pmix_child_num;
extern int *_pmix_child_list;
extern char *_pmix_parent_host;
extern slurm_addr_t *_pmix_parent_addr;


int pmix_info_nodes_list_set(char ***env);
int pmix_info_nodes_env_set(char *this_host, int *childs, int child_cnt);
int  pmix_info_parent_set_root();
int  pmix_info_parent_set_srun(char *phost, uint16_t port);
int pmix_info_parent_set_stepd(char *phost);

inline static parent_type_t pmix_info_parent_type(){
  xassert( _pmix_parent_type != PMIX_PARENT_NONE );
  return _pmix_parent_type;
}

inline static char *pmix_info_step_hosts(){
  // xassert( _pmix_parent_type != PMIX_PARENT_NONE );
  // This information is essential in determinig of the parent type
  // The only thign that's important - did we initialize _pmix_nodes_list itself?
  xassert( _pmix_nodes_list != NULL );
  return _pmix_nodes_list;
}

inline static char *pmix_info_this_host(){
  // xassert( _pmix_parent_type != PMIX_PARENT_NONE );
  // This routine is called from PMIX_DEBUG/ERROR and
  // this CAN happen before initialization. Relax demand to have
  // _pmix_parent_type != PMIX_PARENT_NONE
  if( _pmix_this_host == NULL ){
    return "unknown";
  } else {
    return _pmix_this_host;
  }
}

inline static char *pmix_info_parent_host()
{
  // Check for initialization
  xassert( _pmix_parent_type != PMIX_PARENT_NONE );
  return _pmix_parent_host;
}

inline static slurm_addr_t *pmix_info_parent_addr()
{
  // Check for initialization
  xassert( _pmix_parent_type != PMIX_PARENT_NONE );
  return _pmix_parent_addr;
}

inline static int pmix_info_childs()
{
  // Check for initialization
  xassert( _pmix_parent_type != PMIX_PARENT_NONE );
  return _pmix_child_num;
}

inline static int pmix_info_nth_child(int n)
{
  // Check for initialization
  xassert( _pmix_parent_type != PMIX_PARENT_NONE );
  xassert( n < _pmix_child_num );
  return _pmix_child_list[n];
}
char *pmix_info_nth_child_name(int idx);
char *pmix_info_nth_host_name(int n);

inline static int pmix_info_is_child_no(int id)
{
  // Check for initialization
  xassert( _pmix_parent_type != PMIX_PARENT_NONE );
  int i;
  for(i=0;i<_pmix_child_num; i++){
    if( id == _pmix_child_list[i] ){
      return i;
    }
  }
  return -1;
}

void pmix_info_io_set(eio_handle_t *h);
eio_handle_t *pmix_info_io();

#endif // INFO_H
