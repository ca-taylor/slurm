#ifndef PMIX_DEBUG_H
#define PMIX_DEBUG_H

#include "pmix_common.h"

#define MAX_MSG_SIZE 1024

#define PMIX_DEBUG(format, args...) {           \
  char file[] = __FILE__;                       \
  char *file_base = strrchr(file, '/');         \
  if( file_base == NULL ){                      \
    file_base = file;                           \
  }                                             \
  debug("%s:%d [%s] mpi/pmix: " format "\n",    \
        file_base, __LINE__, __FUNCTION__,      \
        ## args);                               \
}

#define PMIX_ERROR(format, args...) {                 \
  char file[] = __FILE__;                             \
  char *file_base = strrchr(file, '/');               \
  if( file_base == NULL ){                            \
    file_base = file;                                 \
  }                                                   \
  error("%s:%d [%s] mpi/pmix: " format ": %s (%d)\n", \
        file_base, __LINE__, __FUNCTION__,            \
        ## args, strerror(errno), errno);             \
}

#endif // PMIX_DEBUG_H
