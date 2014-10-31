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
  uint32_t node_id;     /* relative position of this node in job     */
  uint32_t node_tasks;  /* number of tasks on *this* node            */
  uint32_t *gtids;
  uint32_t task_dist;
  // TODO: remove later
  //eio_handle_t  *eio; ??
  // List 	       sruns; ?? /* List of srun_info_t pointers               */
  // List           clients; ?? /* List of struct client_io_info pointers   */
};

extern struct pmix_jobinfo_t pmix_job_info;

// Client contact information
void pmix_info_cli_contacts_set(char *path, int fd);
const char *pmix_info_cli_addr();
int pmix_info_cli_fd();

// slurmd contact information
void pmix_info_server_contacts_set(char *path, int fd);
const char *pmix_info_srv_addr();
int pmix_info_srv_fd();

// Agent location
void pmix_info_is_stepd_set();
void pmix_info_is_srun_set();
int pmix_info_is_stepd();
int pmix_info_is_srun();

// Job information
void pmix_info_job_set(const stepd_step_rec_t *job);

inline static uint32_t pmix_info_jobid(){
  xassert(pmix_job_info.magic == PMIX_INFO_MAGIC );
  return pmix_job_info.jobid;
}

inline static uint32_t pmix_info_stepid(){
  xassert(pmix_job_info.magic == PMIX_INFO_MAGIC );
  return pmix_job_info.stepid;
}

inline static uint32_t pmix_info_nodeid(){
  xassert(pmix_job_info.magic == PMIX_INFO_MAGIC );
  return pmix_job_info.node_id;
}

inline static uint32_t pmix_info_nodes(){
  xassert(pmix_job_info.magic == PMIX_INFO_MAGIC );
  return pmix_job_info.nnodes;
}

inline static uint32_t pmix_info_tasks(){
  xassert(pmix_job_info.magic == PMIX_INFO_MAGIC );
  return pmix_job_info.ntasks;
}

inline static uint32_t pmix_info_node_taskcnt(uint32_t i){
  xassert(pmix_job_info.magic == PMIX_INFO_MAGIC );
  xassert( i < pmix_job_info.nnodes);
  return pmix_job_info.task_cnts[i];
}


inline static uint32_t pmix_info_ltasks(){
  xassert(pmix_job_info.magic == PMIX_INFO_MAGIC );
  return pmix_job_info.node_tasks;
}

inline static uint32_t pmix_info_ltask_id(uint32_t i){
  xassert(pmix_job_info.magic == PMIX_INFO_MAGIC );
  xassert( i < pmix_job_info.node_tasks );
  return pmix_job_info.gtids[i];
}


#endif // INFO_H
