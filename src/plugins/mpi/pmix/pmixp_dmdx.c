#include "pmixp_common.h"
#include "pmixp_dmdx.h"
#include "pmixp_server.h"

#include <pmix_server.h>



typedef enum {
	DMDX_REQUEST = 1, DMDX_RESPONSE
} dmdx_type_t;

typedef struct
{
	uint32_t seq_num;
#ifndef NDEBUG
	/* we need this only for verification */
	char nspace[PMIX_MAX_NSLEN];
	int rank;
#endif
	pmix_modex_cbfunc_t cbfunc;
	void *cbdata;
} dmdx_req_info_t;

typedef struct
{
	uint32_t seq_num;
	pmix_proc_t proc;
	char *sender_host, *sender_ns;
	int rank;
} dmdx_caddy_t;

void _dmdx_free_caddy(dmdx_caddy_t *caddy)
{
	if( NULL != caddy->sender_host ){
		xfree(caddy->sender_host);
	}
	if( NULL != caddy->sender_ns ){
		xfree(caddy->sender_ns);
	}
	xfree(caddy);
}

static List _dmdx_requests;
static uint32_t _dmdx_seq_num = 1;

static int _respond_error(char *ns, int rank, char *sender_host, char *sender_ns);

int pmixp_dmdx_init()
{
	_dmdx_requests = list_create(pmixp_xfree_xmalloced);
	_dmdx_seq_num = 1;
	return SLURM_SUCCESS;
}

static void _setup_header(Buf buf, dmdx_type_t t,
			  const char *nspace, int rank, int status)
{
	char *str;
	/* 1. pack message type */
	unsigned char type = (char)t;
	grow_buf(buf,sizeof(char));
	pack8(type,buf);

	/* 2. pack namespace _with_ '\0' (strlen(nspace) + 1)! */
	packmem((char*)nspace, strlen(nspace) + 1, buf);

	/* 3. pack rank */
	grow_buf(buf, sizeof(int) );
	pack32((uint32_t)rank, buf);

    /* 4. pack my rendezvous point - local namespace
	 * ! _with_ '\0' (strlen(nspace) + 1) ! */
	str = pmixp_info_namespace();
	packmem(str,strlen(str) + 1, buf);

	/* 5. pack the status */
	pack32((uint32_t)status, buf);
}

static int _read_type(Buf buf, dmdx_type_t *type)
{
	unsigned char t;
	int rc;
	/* 1. unpack message type */
	if( SLURM_SUCCESS != (rc = unpack8(&t, buf) ) ){
		PMIXP_ERROR("Cannot unpack message type!");
		return SLURM_ERROR;
	}
	*type = (dmdx_type_t)t;
	return SLURM_SUCCESS;
}

static int _read_info(Buf buf, char **ns, int *rank,
		      char **sender_ns, int *status)
{
	uint32_t cnt, uint32_tmp;
	int rc;
	*ns = NULL;
	*sender_ns = NULL;

	/* 1. unpack namespace */
	if( SLURM_SUCCESS != (rc = unpackmem_ptr(ns, &cnt, buf) ) ){
		PMIXP_ERROR("Cannot unpack requested namespace!");
		goto eexit;
	}
	// We supposed to unpack a whole null-terminated string (with '\0')!
	// (*ns)[cnt] = '\0';

	/* 2. unpack rank */
	if( SLURM_SUCCESS != (rc = unpack32(&uint32_tmp, buf) ) ){
		PMIXP_ERROR("Cannot unpack requested rank!");
		goto eexit;
	}
	*rank = uint32_tmp;

	if( SLURM_SUCCESS != (rc = unpackmem_ptr(sender_ns, &cnt, buf) ) ){
		PMIXP_ERROR("Cannot unpack sender namespace!");
		goto eexit;
	}
	// We supposed to unpack a whole null-terminated string (with '\0')!
	// (*sender_ns)[cnt] = '\0';

	/* 4. unpack status */
	if( SLURM_SUCCESS != (rc = unpack32(&uint32_tmp, buf) ) ){
		PMIXP_ERROR("Cannot unpack rank!");
		goto eexit;
	}
	*status = uint32_tmp;
	return SLURM_SUCCESS;
eexit:
	return rc;
}

static int _respond_error(char *ns, int rank, char *sender_host, char *sender_ns)
{
	Buf buf = create_buf(NULL,0);
	char *addr;
	int rc;

	_setup_header(buf,DMDX_RESPONSE,ns,rank,SLURM_ERROR);
	/* generate namespace usocket name */
	addr = pmixp_info_nspace_usock(sender_ns);
	/* send response */
	rc = pmixp_server_send(sender_host, PMIXP_MSG_DMDX, rank, addr,
			       get_buf_data(buf), get_buf_offset(buf));
	if( SLURM_SUCCESS != rc ){
		PMIXP_ERROR("Cannot send direct modex request to %s", sender_host);
	}
	xfree(addr);
	free_buf(buf);
	return rc;
}

static void _dmdx_pmix_cb(pmix_status_t status, char *data,
			  size_t sz, void *cbdata)
{
	dmdx_caddy_t *caddy = (dmdx_caddy_t*)cbdata;
	Buf buf = pmixp_server_new_buf();
	char *addr;
	int rc;

	/* setup response header */
	_setup_header(buf, DMDX_RESPONSE, caddy->proc.nspace, caddy->proc.rank, status);

	/* pack the response */
	packmem(data, sz, buf);

	/* setup response address */
	addr = pmixp_info_nspace_usock(caddy->sender_ns);

	/* send the request */
	rc = pmixp_server_send(caddy->sender_host, PMIXP_MSG_DMDX, caddy->seq_num, addr,
			       get_buf_data(buf), get_buf_offset(buf));
	if( SLURM_SUCCESS != rc ){
		PMIXP_ERROR("Cannot send direct modex request to %s", caddy->sender_host);
	}
	xfree(addr);
	free_buf(buf);
}

int pmixp_dmdx_get(const char *nspace, int rank,
		   pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
	dmdx_req_info_t *req;
	char *addr, *host;
	Buf buf;
	int rc;

	/* need to send the request */
	host = pmixp_nspace_resolve(nspace, rank);
	xassert( NULL != host);
	if( NULL == host ){
		return SLURM_ERROR;
	}

	buf = pmixp_server_new_buf();

	/* setup message header */
	_setup_header(buf, DMDX_REQUEST, nspace, rank, SLURM_SUCCESS);
	/* generate namespace usocket name */
	addr = pmixp_info_nspace_usock(nspace);


	/* send the request */
	rc = pmixp_server_send(host, PMIXP_MSG_DMDX, _dmdx_seq_num, addr,
			       get_buf_data(buf), get_buf_offset(buf));
	if( SLURM_SUCCESS != rc ){
		PMIXP_ERROR("Cannot send direct modex request to %s", host);
	}
	xfree(addr);
	free_buf(buf);

	/* track this request */
	req = xmalloc( sizeof(dmdx_req_info_t) );
	req->seq_num = _dmdx_seq_num;
	req->cbfunc = cbfunc;
	req->cbdata = cbdata;
#ifndef NDEBUG
	strncpy(req->nspace, nspace, PMIX_MAX_NSLEN);
	req->rank = rank;
#endif
	list_append(_dmdx_requests,req);

	/* move to the next request */
	_dmdx_seq_num++;

	return rc;
}

static int _dmdx_req(Buf buf, char *sender_host, uint32_t seq_num)
{
	int rank, rc = SLURM_SUCCESS;
	int status;
	char *ns = NULL, *sender_ns = NULL;
	pmixp_namespace_t *nsptr;
	dmdx_caddy_t *caddy = NULL;

	if( SLURM_SUCCESS != (rc = _read_info(buf, &ns, &rank, &sender_ns, &status)) ){
		goto exit;
	}

	if( 0 != strcmp(ns, pmixp_info_namespace()) ){
		/* request for namespase that is not controlled by this daemon
		 * considered as error. This may change in future.  */
		PMIXP_ERROR("Bad request from %s: asked for nspace = %s, mine is %s",
			    sender_host, ns, pmixp_info_namespace() );
		_respond_error(ns, rank, sender_host, sender_ns);
		goto exit;
	}

	nsptr = pmixp_nspaces_local();
	if( nsptr->ntasks <= rank ){
		PMIXP_ERROR("Bad request from %s: nspace \"%s\" has only %d ranks,"
			    " asked for %d",
			    sender_host, ns, nsptr->ntasks, rank);
		rc = SLURM_ERROR;
		_respond_error(ns, rank, sender_host, sender_ns);
		goto exit;
	}

	/* setup temp structure to handle information fro _dmdx_pmix_cb */
	caddy = xmalloc( sizeof(dmdx_caddy_t) );
	caddy->seq_num = seq_num;

	/* ns is a pointer inside incoming buffer */
	strncpy(caddy->proc.nspace, ns, PMIX_MAX_NSLEN);
	ns = NULL;              /* protect the data */
	caddy->proc.rank = rank;

	/* sender_host was passed from outside - copy it */
	caddy->sender_host = xstrdup(sender_host);
	sender_host = NULL;     /* protect the data */

	/* sender_ns is a pointer inside incoming buffer */
	caddy->sender_ns = xstrdup(sender_ns);
	sender_ns = NULL;

	rc = PMIx_server_dmodex_request(&caddy->proc, _dmdx_pmix_cb, (void*)caddy);
	if( PMIX_SUCCESS != rc ){
		PMIXP_ERROR("Can't request direct modex from libpmix-server, rc = %d", rc);
		_dmdx_free_caddy(caddy);
		rc = SLURM_ERROR;
	}

exit:
	if( SLURM_SUCCESS != rc ){
		/* cleanup in case of error */
		_dmdx_free_caddy(caddy);
	}

	return rc;
}

static int _dmdx_req_cmp(void *x, void *key)
{
	dmdx_req_info_t *req = (dmdx_req_info_t*)x;
	uint32_t seq_num = *((uint32_t*)key);
	return (req->seq_num == seq_num);
}

static int _dmdx_resp(Buf buf, char *sender_host, uint32_t seq_num)
{
	dmdx_req_info_t *req;
	int rank, rc = SLURM_SUCCESS;
	int status;
	char *ns = NULL, *sender_ns = NULL;
	char *data = get_buf_data(buf) + get_buf_offset(buf);
	uint32_t size = remaining_buf(buf);

	/* get the service data */
	if( SLURM_SUCCESS != (rc = _read_info(buf, &ns, &rank, &sender_ns, &status)) ){
		goto exit;
	}
	/* get the modex blob */
	unpackmem_ptr(&data, &size, buf);

	/* find the request tracker */
	ListIterator it = list_iterator_create(_dmdx_requests);
	req = (dmdx_req_info_t *)list_find(it,_dmdx_req_cmp,&seq_num);
	if( NULL == req ){
		// We haven't sent this request!
		PMIXP_ERROR("Received DMDX response for %s:%d from %s that wasn't registered locally!",
			    ns, rank, sender_host);
		list_iterator_destroy(it);
		return SLURM_ERROR;
	}

	/* call back to libpmix-server */
	req->cbfunc(status, data, size, req->cbdata, pmixp_free_Buf, (void*)buf);

	/* release tracker & list iterator */
	req = NULL;
	list_delete_item(it);
	list_iterator_destroy(it);

exit:
	if( SLURM_SUCCESS != rc ){
		free_buf(buf);
	}
	if( NULL != ns ){
		xfree(ns);
	}
	if( NULL != sender_ns ){
		xfree(sender_ns);
	}
	return rc;
}

int pmixp_dmdx_process(Buf buf, char *host, uint32_t seq)
{
	dmdx_type_t type;
	int rc;

	_read_type(buf,&type);
	switch( type ){
	case DMDX_REQUEST:
		rc = _dmdx_req(buf, host, seq);
		break;
	case DMDX_RESPONSE:
		rc = _dmdx_resp(buf, host, seq);
		break;
	default:
		PMIXP_ERROR("Bad request. Skip");
		rc = PMIX_ERROR;
		break;
	}
	/* free buffer protecting data */
	buf->head = NULL;
	free_buf(buf);
	return rc;
}
