#ifndef PMIX_DB_H
#define PMIX_DB_H

#include "pmix_common.h"
#include "pmix_info.h"

typedef struct {
#ifndef NDEBUG
#       define PMIX_MSGSTATE_MAGIC 0xdeadbeef
  int  magic;
#endif
  int *db;
} pmix_db_t;

extern pmix_db_t pmix_db;

static inline void pmix_db_init()
{
  int i;
  pmix_db.magic = PMIX_MSGSTATE_MAGIC;
  uint32_t tasks = pmix_info_tasks();
  pmix_db.db = xmalloc(sizeof(int)*tasks);
  for(i=0;i<tasks;i++){
    pmix_db.db[i] = -1;
  }
}

static inline void pmix_db_add_blob(int taskid, void *blob)
{
  xassert(pmix_db.magic == PMIX_MSGSTATE_MAGIC);
  pmix_db.db[taskid] = *(int*)blob;
}

#endif // PMIX_DB_H
