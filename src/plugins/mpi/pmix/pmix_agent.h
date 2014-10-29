#ifndef AGENT_H
#define AGENT_H

#include "pmix_common.h"

int pmix_agent_start(void);
void pmix_agent_task_cleanup();

#endif // AGENT_H
