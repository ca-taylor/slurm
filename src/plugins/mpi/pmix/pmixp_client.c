/*****************************************************************************\
 **  pmix_client.c - PMIx client communication code
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015      Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com>.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/


#include "pmixp_common.h"
#include "pmixp_state.h"
#include "pmixp_io.h"
#include "pmixp_nspaces.h"
#include "pmixp_debug.h"
#include "pmixp_coll.h"
#include "pmixp_server.h"
#include "pmixp_dmdx.h"

#include <pmix_server.h>

static pmix_status_t
finalize_fn(const char nspace[], int rank, void *server_object,
	    pmix_op_cbfunc_t cbfunc, void *cbdata);
static pmix_status_t
abort_fn(const char nspace[], int rank, void *server_object,
	 int status, const char msg[], pmix_proc_t procs[],
	 size_t nprocs, pmix_op_cbfunc_t cbfunc, void *cbdata);
static pmix_status_t
fencenb_fn(const pmix_proc_t procs[], size_t nprocs,
	   char *data, size_t ndata, pmix_modex_cbfunc_t cbfunc, void *cbdata);
static pmix_status_t
dmodex_fn(const char nspace[], int rank, pmix_modex_cbfunc_t cbfunc, void *cbdata);
static pmix_status_t
publish_fn(const char nspace[], int rank, pmix_data_range_t scope,
	   pmix_persistence_t persist, const pmix_info_t info[], size_t ninfo,
	   pmix_op_cbfunc_t cbfunc, void *cbdata);
static pmix_status_t
lookup_fn(const char nspace[], int rank, pmix_data_range_t scope, int wait,
	  char **keys, pmix_lookup_cbfunc_t cbfunc, void *cbdata);
static pmix_status_t
unpublish_fn(const char nspace[], int rank, pmix_data_range_t scope,
	     char **keys, pmix_op_cbfunc_t cbfunc, void *cbdata);
static pmix_status_t
spawn_fn(const pmix_app_t apps[], size_t napps, pmix_spawn_cbfunc_t cbfunc,
	 void *cbdata);
static pmix_status_t
connect_fn(const pmix_proc_t procs[], size_t nprocs, pmix_op_cbfunc_t cbfunc,
	   void *cbdata);
static pmix_status_t
disconnect_fn(const pmix_proc_t procs[], size_t nprocs, pmix_op_cbfunc_t cbfunc,
	      void *cbdata);

pmix_server_module_t _slurm_pmix_cb = {
	finalize_fn,
	abort_fn,
	fencenb_fn,
	dmodex_fn,
	publish_fn,
	lookup_fn,
	unpublish_fn,
	spawn_fn,
	connect_fn,
	disconnect_fn,
	NULL
};

static void errhandler(pmix_status_t status, pmix_proc_t proc[],
		       size_t nproc, pmix_info_t info[], size_t ninfo);

int pmixp_libpmix_init()
{
	int rc;

	/* TODO: remove this once debugged! */
	PMIXP_ERROR("WARNING: you are using /home/artpol/slurm_tmp/ as tmpdir!");
	setenv("TMPDIR","/home/artpol/slurm_tmp/",1);

	/* setup the server library */
	if (PMIX_SUCCESS != (rc = PMIx_server_init(&_slurm_pmix_cb, true))) {
		PMIXP_ERROR_STD("PMIx_server_init failed with error %d\n", rc);
		return SLURM_ERROR;
	}

	/* register the errhandler */
	PMIx_Register_errhandler(errhandler);

	return 0;
}

int pmixp_libpmix_finalize()
{
	if( PMIX_SUCCESS != PMIx_server_finalize() ){
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

static void errhandler(pmix_status_t status,
		       pmix_proc_t proc[], size_t nproc,
		       pmix_info_t info[], size_t ninfo)
{
	// TODO: do something more sophisticated here
	// FIXME: use proper specificator for nranges
	PMIXP_ERROR_STD("Error handler invoked: status = %d, nranges = %d", status, (int)nproc);
	slurm_kill_job_step(pmixp_info_jobid(), pmixp_info_stepid(), SIGKILL);
}

#define PMIXP_ALLOC_KEY(kvp, key_str) {				\
	char *key = key_str;					\
	kvp = (pmix_info_t*)xmalloc(sizeof(pmix_info_t));	\
	(void)strncpy(kvp->key, key, PMIX_MAX_KEYLEN);		\
	}

static void _release_cb(pmix_status_t status, void *cbdata)
{
	int *ptr = (int*)cbdata;
	*ptr = 0;
}

int pmixp_libpmix_job_set()
{
	List lresp;
	pmix_info_t *info;
	int ninfo;
	char *p = NULL;
	ListIterator it;
	pmix_info_t *kvp;
	uint32_t tmp;
	int i, rc;
	uid_t uid = getuid();
	gid_t gid = getgid();
	int in_progress = 1;

	// Use list to safely expand/reduce key-value pairs.
	lresp = list_create(pmixp_xfree_buffer);

	/* The name of the current host */
	PMIXP_ALLOC_KEY(kvp, PMIX_HOSTNAME);
	PMIX_VAL_SET(&kvp->value, string, pmixp_info_hostname());
	list_append(lresp, kvp);

	/* Setup temprary directory */
	/* TODO: consider all ways of setting TMPDIR here: fixed, prolog, what else? */
	p = getenv("TMPDIR");
	if( NULL == p ){
		p = "/tmp/";
	}
	PMIXP_ALLOC_KEY(kvp, PMIX_TMPDIR);
	PMIX_VAL_SET(&kvp->value, string, p);
	list_append(lresp, kvp);

	/* jobid assigned by scheduler */
	p = NULL;
	xstrfmtcat(p, "%d.%d", pmixp_info_jobid(), pmixp_info_stepid());
	PMIXP_ALLOC_KEY(kvp, PMIX_JOBID);
	PMIX_VAL_SET(&kvp->value, string, p);
	xfree(p);
	list_append(lresp, kvp);

	/* process identification ranks */
	// TODO: what to put here for slurm?
	// FIXME: This information supposed to be related to the precise rank
	/*
	PMIXP_ALLOC_KEY(kvp, PMIX_GLOBAL_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_task_id(taskid));
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_task_id(taskid));
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_LOCAL_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, taskid);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_APP_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_task_id(taskid));
	list_append(lresp, kvp);
*/
	PMIXP_ALLOC_KEY(kvp, PMIX_APPNUM);
	PMIX_VAL_SET(&kvp->value, uint32_t, 0);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_NODE_RANK);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_nodeid());
	list_append(lresp, kvp);

	/* size information */
	PMIXP_ALLOC_KEY(kvp, PMIX_UNIV_SIZE);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks_uni());
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_JOB_SIZE);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks());
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_LOCAL_SIZE);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks_loc());
	list_append(lresp, kvp);

	// TODO: fix it in future
	PMIXP_ALLOC_KEY(kvp, PMIX_NODE_SIZE);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks_loc());
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_MAX_PROCS);
	PMIX_VAL_SET(&kvp->value, uint32_t, pmixp_info_tasks_uni());
	list_append(lresp, kvp);

	/* offset information */
	// TODO: Fix this in future once Spawn will be implemented
	PMIXP_ALLOC_KEY(kvp, PMIX_NPROC_OFFSET);
	PMIX_VAL_SET(&kvp->value, uint32_t, 0);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_APPLDR);
	PMIX_VAL_SET(&kvp->value, uint32_t, 0);
	list_append(lresp, kvp);

	xstrfmtcat(p,"%u", pmixp_info_taskid(0));
	tmp = pmixp_info_taskid(0);
	for(i=1;i< pmixp_info_tasks_loc();i++){
		uint32_t rank = pmixp_info_taskid(i);
		xstrfmtcat(p,",%u", rank);
		if( tmp > rank ){
			tmp = rank;
		}
	}

	PMIXP_ALLOC_KEY(kvp, PMIX_LOCAL_PEERS);
	PMIX_VAL_SET(&kvp->value, string, p);
	xfree(p);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_LOCALLDR);
	PMIX_VAL_SET(&kvp->value, uint32_t, tmp);
	list_append(lresp, kvp);

	PMIXP_ALLOC_KEY(kvp, PMIX_PROC_MAP);
	PMIX_VAL_SET(&kvp->value, string, pmixp_info_task_map());
	list_append(lresp, kvp);

	ninfo = list_count(lresp);
	info = xmalloc(sizeof(pmix_info_t) * ninfo);
	it = list_iterator_create(lresp);
	i = 0;
	while( NULL != (kvp = list_next(it) ) ){
		info[i] = *kvp;
		i++;
	}
	list_destroy(lresp);

	in_progress = 1;
	rc = PMIx_server_register_nspace(pmixp_info_namespace(), pmixp_info_tasks_loc(),
					 info, ninfo, _release_cb, &in_progress);
	if (PMIX_SUCCESS == rc ) {
		while( in_progress ){
			struct timespec ts;
			ts.tv_sec = 0;
			ts.tv_nsec = 100;
			nanosleep(&ts,NULL);
		}
	}
	xfree(info);
	if( PMIX_SUCCESS != rc ){
		PMIXP_ERROR("Cannot register namespace %s, nlocalproc=%d, "
			    "ninfo = %d", pmixp_info_namespace(),
			    pmixp_info_tasks_loc(), ninfo );
		return SLURM_ERROR;
	}

	PMIXP_DEBUG("task initialization");
	for(i=0;i<pmixp_info_tasks_loc();i++){
		in_progress = 1;
		rc = PMIx_server_register_client(pmixp_info_namespace(),
						 pmixp_info_taskid(i), uid, gid, NULL,
						 _release_cb, &in_progress);
		if (PMIX_SUCCESS == rc ) {
			while( in_progress ){
				struct timespec ts;
				ts.tv_sec = 0;
				ts.tv_nsec = 100;
				nanosleep(&ts,NULL);
			}
		}
		if( PMIX_SUCCESS != rc ){
			PMIXP_ERROR("Cannot register client %d(%d) in namespace %s",
				    pmixp_info_taskid(i), i, pmixp_info_namespace() );
			return SLURM_ERROR;
		}
	}
	return SLURM_SUCCESS;
}

static int finalize_fn(const char nspace[], int rank, void* server_object,
		       pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	// TODO: think what we supposed to do here.
	PMIXP_DEBUG("called");

	cbfunc(PMIX_SUCCESS, cbdata);
	return PMIX_SUCCESS;
}

pmix_status_t abort_fn(const char nspace[], int rank,
		       void *server_object,
		       int status, const char msg[],
		       pmix_proc_t procs[], size_t nprocs,
		       pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	// Just kill this stepid for now. Think what we can do for FT here?
	PMIXP_DEBUG("called: status = %d, msg = %s", status, msg);
	slurm_kill_job_step(pmixp_info_jobid(), pmixp_info_stepid(), SIGKILL);

	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, cbdata);
	}
	return PMIX_SUCCESS;
}

pmix_status_t fencenb_fn(const pmix_proc_t procs[], size_t nprocs,
			 char *data, size_t ndata,
			 pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	pmixp_coll_t *coll;
	pmixp_coll_type_t type = PMIXP_COLL_TYPE_FENCE;
	pmix_status_t status = PMIX_SUCCESS;

	pmixp_debug_hang(0);

	coll = pmixp_state_coll_get(type, procs, nprocs);
	pmixp_coll_set_callback(coll, cbfunc, cbdata);
	if( SLURM_SUCCESS != pmixp_coll_contrib_local(coll, data, ndata) ){
		goto error;
	}
	return PMIX_SUCCESS;
error:
	cbfunc(status,NULL,0,cbdata);
	return status;
}

pmix_status_t dmodex_fn(const char nspace[], int rank,
			pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_IMPLEMENTED;
}

static pmix_status_t
publish_fn(const char nspace[], int rank, pmix_data_range_t scope,
	   pmix_persistence_t persist, const pmix_info_t info[], size_t ninfo,
	   pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_IMPLEMENTED;
}

static pmix_status_t
lookup_fn(const char nspace[], int rank, pmix_data_range_t scope, int wait,
	  char **keys, pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_IMPLEMENTED;
}

static pmix_status_t
unpublish_fn(const char nspace[], int rank, pmix_data_range_t scope,
	     char **keys, pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_IMPLEMENTED;
}

static int spawn_fn(const pmix_app_t apps[], size_t napps,
		    pmix_spawn_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_IMPLEMENTED;
}

static pmix_status_t
connect_fn(const pmix_proc_t procs[], size_t nprocs, pmix_op_cbfunc_t cbfunc,
	   void *cbdata){
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_IMPLEMENTED;
}

static pmix_status_t
disconnect_fn(const pmix_proc_t procs[], size_t nprocs, pmix_op_cbfunc_t cbfunc,
	      void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_IMPLEMENTED;
}

