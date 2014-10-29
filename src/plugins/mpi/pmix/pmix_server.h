#ifndef SERVER_H
#define SERVER_H

#include "pmix_common.h"


void pmix_server_request(eio_handle_t *h, int fd);

#endif // SERVER_H
