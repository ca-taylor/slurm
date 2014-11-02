#ifndef CLIENT_H
#define CLIENT_H

#include "pmix_common.h"



void pmix_client_request(int fd);
void pmix_client_fence_notify();


#endif // CLIENT_H
