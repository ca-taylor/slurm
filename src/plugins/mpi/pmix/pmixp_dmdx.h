#ifndef PMIXP_DMDX_H
#define PMIXP_DMDX_H

#include "pmixp_common.h"
#include "pmixp_nspaces.h"

int pmixp_dmdx_init();
int pmixp_dmdx_get(const char *nspace, int rank,
	   pmix_modex_cbfunc_t cbfunc, void *cbdata);
void pmixp_dmdx_process(Buf buf, char *host, uint32_t seq);

#endif // PMIXP_DMDX_H
