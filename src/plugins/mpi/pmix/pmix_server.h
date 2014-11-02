#ifndef SERVER_H
#define SERVER_H

#include "pmix_common.h"

int pmix_stepd_init(const stepd_step_rec_t *job, char ***env);
int pmix_srun_init(const mpi_plugin_client_info_t *job, char ***env);
void pmix_server_request(int fd);
char *pmix_server_alloc_msg(uint32_t size, char **payload);
void pmix_server_msg_set_fence(char *msg);
void pmix_server_msg_set_fence_resp(char *msg);
uint32_t pmix_server_sendmsg_size(char *msg);

#endif // SERVER_H
