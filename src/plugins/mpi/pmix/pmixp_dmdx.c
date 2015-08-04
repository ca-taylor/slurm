#include "pmixp_common.h"
#include "pmixp_dmdx.h"
#include "pmixp_server.h"

typedef enum {
    DMDX_REQUEST = 1, DMDX_RESPONSE
} dmdx_type_t;

typedef struct {
	List rem_req;
	List *loc_req;
} pmixp_dmdx_t;

static pmixp_dmdx_t _dmdx;

typedef struct{
	char *nspace;
	int rank;
	List resp_list;
} req_to_rem_t;

typedef struct
{
    pmix_modex_cbfunc_t cbfunc;
    void *cbdata;
} callback_info_t;

void _free_req_to_rem(void *x)
{
    req_to_rem_t *req = x;
    xfree(req->nspace);
    list_destroy(req->resp_list);
    xfree(req);
}

typedef struct{
    char *host, *addr;
} req_to_loc_t;

void _free_req_to_loc(void *x)
{
    req_to_loc_t *req = x;
    xfree(req->addr);
    xfree(req->host);
    xfree(req);
}

static
static void _account_remote_req(char *nspace, int rank,
                             pmix_modex_cbfunc_t cbfunc, void *cbdata);
static void _wait_for_local(int rank, char *host, char *addr);
static int _respond_bad_param(char *nspace, char *host);
static int _respond_ok(char *nspace, char *host, List modex_data);

void pmixp_dmdx_init()
{
	int localnum = pmixp_info_tasks_loc();
	int i;
    _dmdx.rem_req = list_create(_free_req_to_rem);
	_dmdx.loc_req = xmalloc( sizeof(List));
	for(i=0; i< localnum; i++){
        _dmdx.loc_req[i] = list_create(_free_req_to_loc);
	}
}

static void _setup_header(Buf buf, dmdx_type_t t, char *nspace,
                          int rank, int status)
{
    char type = (char)t;
    /* 1. pack message type */
    grow_buf(buf,sizeof(char));
    pack8(type,buf);
    /* 2. pack namespace */
    grow_buf(buf, strlen(nspace) + 1 );
    packmem(nspace, strlen(nspace), buf);
    /* 3. pack rank */
    grow_buf(buf, sizeof(int) );
    pack32((uint32_t)rank, buf);
    /* 4. pack our contact info */
    str = pmixp_info_hostname();
    grow_buf(buf, strlen(str));
    packmem(str,strlen(str) + 1, buf);
    /* 5. pack our contact info */
    grow_buf(buf, sizeof(int) );
    pack32((uint32_t)status, buf);
}

static int _read_type(Buf buf, dmdx_type_t *type)
{
    char t;
    /* 1. unpack message type */
    if( SLURM_SUCCESS != (rc = unpack8(&t, buf) ) ){
        PMIXP_ERROR("Cannot unpack message type!");
        goto rc;
    }
    *type = (dmdx_type_t)t;
    return SLURM_SUCCESS;
}

static void _read_info(Buf buf, char **nspace, int *rank,
                            char **host, int *status)
{
    int cnt;
    *nspace = NULL;
    *host = NULL;
    /* 2. unpack namespace */
    if( SLURM_SUCCESS != (rc = unpackmem_xmalloc(nspace, &cnt, buf) ) ){
        PMIXP_ERROR("Cannot unpack namespace!");
        goto eexit;
    }
    nspace[cnt] = '\0';
    /* 3. unpack rank */
    if( SLURM_SUCCESS != (rc = unpack32(rank, buf) ) ){
        PMIXP_ERROR("Cannot unpack rank!");
        goto eexit;
    }
    /* 4. unpack message type */
    if( SLURM_SUCCESS != (rc = unpackmem_xmalloc(host, &cnt, buf) ) ){
        PMIXP_ERROR("Cannot unpack hostname!");
        goto eexit;
    }
    host[cnt] = '\0';

    /* 5. unpack status */
    if( SLURM_SUCCESS != (rc = unpack32(status, buf) ) ){
        PMIXP_ERROR("Cannot unpack rank!");
        goto eexit;
    }
    return SLURM_SUCCESS;
eexit:
    if( NULL != *nspace){
        xfree(*nspace);
    }
    if( NULL != *host ){
        xfree(*host);
    }
    return rc;
}

static int _account_remote_req(char *nspace, int rank,
                             pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
    req_to_rem_t *req;
    callback_info_t *cbi = xmalloc(callback_info_t);
    ListIterator it;
    bool found;
    cbi->cbfunc = cbfunc;
    cbi->cbdata = cbdata;

    it = list_iterator_create(_dmdx.rem_req);
    while( NULL != ( req = list_next(it) ) ){

        if( 0 != strcmp(nspace, req->nspace) ){
            continue;
        }
        if( rank != req->rank ){
            continue;
        }

        list_append(req->resp_list, cbi);
        found = true;
        break;
    }
    list_iterator_destroy(it);

    if( found ){
        /* not fatal, will need to add new request */
        return SLURM_SUCCESS;
    }

    /* setup direct modex request */
    req = xmalloc( sizeof(req_to_rem_t) );
    req->nspace = strdup(nspace);
    req->rank = rank;
    list_append(req->resp_list, cbi);
    /* add direct modex request */
    list_append(_dmdx.rem_req, req);
    /* let caller know that it needs to send request */
    return SLURM_ERROR;
}

static void _wait_for_local(int rank, char *host, char *addr)
{
    req_to_loc_t *req = xmalloc(sizeof(req_to_loc_t));
#ifndef NDEBUG
    pmixp_namespace_t *nsptr = pmixp_nspaces_local();
    xassert( NULL != nsptr );
    xassert( rank < nsptr->ntasks );
#endif
    req->host = host;
    req->addr = addr;
    list_append(_dmdx.loc_req[rank], req);
}

static int _respond_bad_param(char *nspace, char *host)
{
    Buf buf = create_buf(NULL,0);
    char *addr;
    _setup_header(buf,DMDX_RESPONSE,nspace,0,SLURM_ERROR);
    /* generate namespace usocket name */
    addr = pmixp_info_nspace_usock(nspace);
    /* send response */
    rc = pmixp_server_send(host, PMIX_MSG_DIREQ, 0, addr,
              buf->head, get_buf_offset(buf));
    if( SLURM_SUCCESS != rc ){
        PMIXP_ERROR("Cannot send direct modex request to %s", host);
    }
    xfree(addr);
    free_buf(buf);

}

static int _respond_ok(char *nspace, char *host, List modex_list)
{
    Buf buf = create_buf(NULL,0);
    char *addr;
    size_t size;
    _setup_header(buf,DMDX_RESPONSE,nspace,0,SLURM_ERROR);
    /* save modex data */
    size = pmixp_nspace_mdx_lsize(modex_list);
    grow_buf(buf,sizeof(uint32_t));
    pack32((uint32_t)list_count(modex_list), buf);
    grow_buf(buf,size);
    pmixp_nspaces_pack_modex(buf, modex_list);

    /* generate namespace usocket name */
    addr = pmixp_info_nspace_usock(nspace);
    /* send the request */
    rc = pmixp_server_send(host, PMIX_MSG_DIREQ, 0, addr,
              buf->head, get_buf_offset(buf));
    if( SLURM_SUCCESS != rc ){
        PMIXP_ERROR("Cannot send direct modex request to %s", host);
    }
    xfree(addr);
    free_buf(buf);

}


int pmixp_dmdx_get(char *nspace, int rank,
		   pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
    int rc;
	char *host;
	Buf buf;
	size_t size;
	char *str, *addr;

    rc = _account_remote_req(nspace, rank, cbfunc, cbdata);
    if( SLURM_SUCCESS == rc ){
        /* we already requested this data */
        return rc;
    }

    /* need to send the request */
	host = pmixp_nspace_resolve(nspace, rank);
	xassert( NULL != host);
	if( NULL == host ){
		return SLURM_ERROR;
	}

	buf = create_buf(NULL, 0);
    /* setup message header */
    _setup_header(buf, DMDX_REQUEST, nspace, rank, SLURM_SUCCESS);
    /* generate namespace usocket name */
    addr = pmixp_info_nspace_usock(nspace);
    /* send the request */
	rc = pmixp_server_send(host, PMIX_MSG_DIREQ, 0, addr,
			  buf->head, get_buf_offset(buf));
	if( SLURM_SUCCESS != rc ){
		PMIXP_ERROR("Cannot send direct modex request to %s", host);
	}
    xfree(addr);
	free_buf(buf);

	return rc;
}

static int _dmdx_req(pmixp_dmdx_t *dmdx, Buf buf)
{
    Buf buf = create_buf(msg, size);
    uint32_t cnt;
    int rank, rc = SLURM_SUCCESS;
    int status;
    char *nspace = NULL, *host = NULL, *addr = NULL;
	pmixp_namespace_t *nsptr;
	List modex_data;

    if( SLURM_SUCCESS != (rc = _read_info(buf,&nspace, &rank, &host, &status)) ){
        goto exit;
    }

	if( 0 != strcmp(nspace, pmixp_info_namespace()) ){
		PMIXP_ERROR("Bad request from %s: asked for nspace = %s, mine is %s",
			    host, nspace, pmixp_info_namespace() );
        _respond_bad_param(nspace, rank, host);
        goto exit;
	}

    nsptr = pmixp_nspaces_local();
	if( nsptr->ntasks <= rank ){
		PMIXP_ERROR("Bad request from %s: nspace \"%s\" has only %d ranks,"
			    " asked for %d",
			    host, nspace, nsptr->ntasks, rank);
        rc = SLURM_ERROR;
        _respond_bad_param(host,addr);
        goto exit;
	}

	modex_data = list_create(pmixp_xfree_buffer);
    pmixp_nspace_rank_blob(nsptr,PMIX_GLOBAL,rank,modex_data);
    pmixp_nspace_rank_blob(nsptr,PMIX_REMOTE,rank,modex_data);

    if( 0 == list_count(modex_data) ){
        /* need to wait for the local data to arrive */
        _wait_for_local(nspace, rank, host, addr);
        /* _wait_for_local will use allocated strings host, addr
         * so we don't want to free them on exit */
        host = NULL;
        addr = NULL;
    } else {
        _respond_ok(nspace, host, modex_data);
    }

exit:
    if( NULL != nspace ){
        xfree(nspace);
    }
    if( NULL != host ){
        xfree(host);
    }
    if( NULL != host ){
        xfree(host);
    }
    return rc;
}

void _dmdx_resp(pmixp_dmdx_t *dmdx, void *msg, size_t size)
{
    Buf buf = create_buf(msg, size);
    uint32_t cnt;
    int rank, rc = SLURM_SUCCESS;
    int status;
    char *nspace = NULL, *host = NULL, *addr = NULL;
    pmixp_namespace_t *nsptr;

    if( SLURM_SUCCESS != (rc = _read_info(buf,&nspace, &rank, &host, &status)) ){
        goto exit;
    }

    /* unpack count */
    if( SLURM_SUCCESS != (rc = unpack32(&cnt, buf) ) ){
        PMIXP_ERROR("Cannot unpack blob count!");
        goto exit;
    }

    /* push the data into database */
    rc = pmixp_nspaces_push(buf,nspace, cnt);

    _match_pending(nspace,rank);

exit:
    /* protect the data */
    buf->head = NULL;
    free_buf(buf);

    if( NULL != nspace ){
        xfree(nspace);
    }
    if( NULL != host ){
        xfree(host);
    }
    if( NULL != host ){
        xfree(host);
    }
    return rc;
}

int pmixp_dmdx_process(void *msg, size_t size)
{
    dmdx_type_t type;
    Buf buf = create_buf(msg, size);
    _read_type(buf,&type);
    switch( type ){
    case DMDX_REQUEST:
        _dmdx_req(buf);
        break;
    case DMDX_RESPONSE:
        _dmdx_resp(buf);
        break;
    default:
        PMIXP_ERROR("Bad request. Skip");
        break;
    }
}
