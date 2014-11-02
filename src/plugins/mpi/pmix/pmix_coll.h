#ifndef PMIX_COLL_H
#define PMIX_COLL_H
#include "pmix_common.h"

int pmix_coll_init(char ***env);
void pmix_coll_node_contrib(uint32_t nodeid, void *msg, uint32_t size);
void pmix_coll_task_contrib(uint32_t taskid, void *msg, uint32_t size);

#endif // PMIX_COLL_H
