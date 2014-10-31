#include "mpi_pmix.h"


#define MSGSIZE 100

void pmix_server_request(eio_handle_t *h, int fd)
{
  PMIX_DEBUG("Request from fd = %d", fd);
  char buf[1024];
  int i = 0, cnt = 0;
  while( i < MSGSIZE ){
    cnt = read(fd, buf + i, MSGSIZE - i);
    if( cnt < 0 ){
      PMIX_ERROR("Reading server request");
      break;
    }
    i += cnt;
  }

  PMIX_DEBUG("Received %s", buf);
}
/*
pmix_server_init(const mpi_plugin_client_info_t *job)
{
  if (net_stream_listen(&tree_sock,
            &tree_info.pmi_port) < 0) {
    error("mpi/pmi2: Failed to create tree socket");
    return SLURM_ERROR;
  }
  debug("mpi/pmi2: srun pmi port: %hu", tree_info.pmi_port);
}

static int
_setup_srun_socket(const mpi_plugin_client_info_t *job)
{


  return SLURM_SUCCESS;
}

static int
_setup_srun_environ(const mpi_plugin_client_info_t *job, char ***env)
{
   ifhn will be set in SLURM_SRUN_COMM_HOST by slurmd
  env_array_overwrite_fmt(env, PMI2_SRUN_PORT_ENV, "%hu",
        tree_info.pmi_port);
  env_array_overwrite_fmt(env, PMI2_STEP_NODES_ENV, "%s",
        job_info.step_nodelist);
  env_array_overwrite_fmt(env, PMI2_PROC_MAPPING_ENV, "%s",
        job_info.proc_mapping);
  return SLURM_SUCCESS;
}
*/
