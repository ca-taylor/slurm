#include "mpi_pmix.h"
#include "pmix_info.h"
#include "pmix_state.h"

pmix_state_t pmix_state;

void pmix_state_init()
{
  size_t size, i;
#ifndef NDEBUG
  pmix_state.magic = PMIX_STATE_MAGIC;
#endif
  pmix_state.cli_size = pmix_info_ltasks();
  size = pmix_state.cli_size * sizeof(client_state_t);
  pmix_state.cli_state = xmalloc( size );
  for( i = 0; i < pmix_state.cli_size; i++ ){
    pmix_state.cli_state[i].fd = -1;
  }
  //pmix_state.coll.in_progress = false;
}

