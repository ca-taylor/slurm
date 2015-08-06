#ifndef PMIXP_DMDX_H
#define PMIXP_DMDX_H

#include "pmixp_common.h"
#include "pmixp_nspaces.h"

void pmixp_dmdx_get(char *nspace, int rank,
		    pmix_modex_cbfunc_t cbfunc, void *cbdata);
void pmixp_dmdx_process(void *msg, size_t size);

#endif // PMIXP_DMDX_H
