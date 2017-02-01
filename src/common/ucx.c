#include <ucp/api/ucp.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>


#include "src/common/ucx.h"
#include "slurm/slurm_errno.h"

#include "src/common/macros.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/xassert.h"

unsigned long my_ucx_addr_len;
char *my_ucx_addr;
char my_ucx_hostname[1024];
slurm_ucx_type_t _my_type;
/* UCP handler objects */
ucp_context_h ucp_context;
ucp_worker_h ucp_worker;

/* server globals */
static slurm_ucx_srv_cb_t server_cb;
static void *server_obj;

#define _TBL_MAX 1024

bool __initialized = false;

struct ucx_context {
	int completed;
};

typedef struct {
	int fd;
	uint64_t tag;
	int in_use;
	int ep_idx;
	slurm_ucx_cli_cb_t cb;
	void *obj;
} _ucx_conn_table_t;
_ucx_conn_table_t _conns[_TBL_MAX];

typedef struct {
	char sname[256];
	ucp_ep_h ep;
	int in_use;
} _ucx_ep_table_t;
static _ucx_ep_table_t _ep_tbl[_TBL_MAX];

#define TAG_RESP_FLAG 0x100000000

typedef struct {
	struct ucx_context *req;
	char *buf;
	size_t size;
	uint64_t tag;
	int in_use;
} _requests_t;

static _requests_t _rtbl[_TBL_MAX];
static int _rtbl_last = 0;

static _requests_t _stbl[_TBL_MAX];
static int _stbl_last = 0;



static void request_init(void *request)
{
	struct ucx_context *ctx = (struct ucx_context *) request;
	ctx->completed = 0;
}

static void send_handle(void *request, ucs_status_t status)
{
	struct ucx_context *context = (struct ucx_context *) request;
	context->completed = 1;
}

static void recv_handle(void *request, ucs_status_t status,
			ucp_tag_recv_info_t *info)
{
	struct ucx_context *context = (struct ucx_context *) request;
	context->completed = 1;
}


static int slurm_ucx_init()
{
	ucp_config_t *config;
	ucs_status_t status;
	ucp_params_t ucp_params;
	ucp_worker_params_t worker_params;
	ucp_address_t *local_addr;
	unsigned long local_addr_len;
	int i;

	status = ucp_config_read(NULL, NULL, &config);
	if (status != UCS_OK) {
		fprintf(stderr, "Failed ucp_config_read\n");
		exit(-1);
	}

	ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_WAKEUP;
	ucp_params.request_size    = sizeof(struct ucx_context);
	ucp_params.request_init    = request_init;
	ucp_params.request_cleanup = NULL;
	ucp_params.field_mask      = UCP_PARAM_FIELD_FEATURES |
		UCP_PARAM_FIELD_REQUEST_SIZE |
		UCP_PARAM_FIELD_REQUEST_INIT |
		UCP_PARAM_FIELD_REQUEST_CLEANUP;

	status = ucp_init(&ucp_params, config, &ucp_context);
	ucp_config_release(config);
	if (status != UCS_OK) {
		fprintf(stderr, "Failed ucp_config_release\n");
		exit(-1);
	}

	worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
	worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

	status = ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
	if (status != UCS_OK) {
		fprintf(stderr, "ucp_worker_create Failed\n");
		exit(-1);
	}

	status = ucp_worker_get_address(ucp_worker, &local_addr, &local_addr_len);
	if (status != UCS_OK) {
		fprintf(stderr, "ucp_worker_get_address  Failed\n");
		exit(-1);
	}
	my_ucx_addr_len = local_addr_len;
	my_ucx_addr = (char *) local_addr;
	gethostname(my_ucx_hostname, sizeof(my_ucx_hostname));

	for(i=0; i < _TBL_MAX; i++){
		_conns[i].in_use = 0;
		_ep_tbl[i].in_use = 0;
		_rtbl[i].in_use = 0;
		_stbl[i].in_use = 0;
	}

	__initialized = true;

	return SLURM_SUCCESS;

}

void slurm_ucx_cleanup()
{
	ucp_worker_release_address(ucp_worker, (ucp_address_t *)my_ucx_addr);
	ucp_worker_destroy(ucp_worker);
	ucp_cleanup(ucp_context);
}

static int slurm_ucx_save_server_address()
{
	char fname[256];
	FILE *f;
	int i;

	sprintf(fname, "/tmp/slurm-ucx-%s.txt", my_ucx_hostname);
	f = fopen(fname, "w");
	if (f == NULL)
	{
		printf("Error opening file!\n");
		exit(1);
	}

	fprintf(f, "%s\n",my_ucx_hostname);
	fprintf(f, "%lu\n",my_ucx_addr_len);
	for (i = 0; i <my_ucx_addr_len; i++)
		fprintf(f, "%hhx ", my_ucx_addr[i]);
	fprintf(f, "\n");

	fclose(f);
	return 0;
}

static int _get_server_address(char *server_name, char **server_addr, int *server_addr_len)
{
	char fname[256];
	char sname[256];
	char *saddr;
	FILE *f;
	int i;

	sprintf(fname, "/tmp/slurm-ucx-%s.txt", server_name);
	f = fopen(fname, "r");
	if (f == NULL)
	{
		printf("Error opening file!\n");
		return SLURM_ERROR;
	}

	printf("Opening file: %s \n", fname);

	fscanf(f, "%s\n", sname);
	fscanf(f, "%d\n", server_addr_len);
	printf("server name:%s addr_len:%d \n", sname, *server_addr_len);

	saddr = malloc (*server_addr_len);
	printf("server addr :  ");
	for (i = 0; i < *server_addr_len; i++) {
		fscanf(f, "%hhx", &saddr[i]);
		fprintf(stdout, "%hhx ", saddr[i]);
	}
	fprintf(stdout, "\n");
	fclose(f);

	*server_addr = saddr;

	return SLURM_SUCCESS;

}

static int _connect_to_peer(char *sa, ucp_ep_h *ep)
{
	ucp_ep_params_t ep_params;
	int rc;

	ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
	ep_params.address    = (ucp_address_t *) sa;

	rc = ucp_ep_create(ucp_worker, &ep_params, ep);
	if (rc != UCS_OK) {
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

static int _connect_to_srv(char *sname)
{
	char *sa;
	int sa_len, i;

	for(i=0; i< _TBL_MAX; i++){
		if( _ep_tbl[i].in_use && !strcmp(_ep_tbl[i].sname, sname)){
			/* found in local cache */
			return 1;
		}
	}


	if( SLURM_SUCCESS != _get_server_address(sname, &sa, &sa_len) ){
		return SLURM_ERROR;
	}

	for(i=0; i < _TBL_MAX; i++){
		if( !_ep_tbl[i].in_use ){
			break;
		}
	}

	xassert( _TBL_MAX >= i );

	 if( SLURM_SUCCESS != _connect_to_peer(sa, &_ep_tbl[i].ep) ){
		return SLURM_ERROR;
	 }

	_ep_tbl[i].in_use = 1;
	strcpy(_ep_tbl[i].sname, sname);
	return i;
}

static int _connect_to_cli(slurm_ucx_address_t *addr)
{
	int i;
	for(i=0; i< _TBL_MAX; i++){
		if( !_ep_tbl[i].in_use ){
			/* found spot */
			break;
		}
	}

	xassert( i < _TBL_MAX);

	if( SLURM_SUCCESS != _connect_to_peer(addr->addr.address.data, &_ep_tbl[i].ep)){
		return SLURM_ERROR;
	}

	_ep_tbl[i].in_use = 1;
	_ep_tbl[i].sname[0] = 0;
	return i;
}

int slurm_ucx_reachable(slurm_ucx_address_t *addr)
{
	if( SLURM_UCX_CLI == addr->type ){
		return 0;
	}

	/* cache it now - we will use it! */
	if( SLURM_SUCCESS == _connect_to_srv( addr->addr.hname) ){
		return 1;
	}

	return 0;
}


int slurm_ucx_addr(slurm_ucx_address_t *addr)
{
	addr->type = _my_type;

	switch( _my_type ){
	case SLURM_UCX_CLI:
		addr->addr.address.data = my_ucx_addr;
		addr->addr.address.len = my_ucx_addr_len;
		break;
	case SLURM_UCX_SRV:
		addr->addr.hname = my_ucx_hostname;
		break;
	}
	return SLURM_SUCCESS;
}

slurm_ucx_type_t slurm_ucx_whoami()
{
	return _my_type;
}

int slurm_ucx_init_server(char *fname, slurm_ucx_srv_cb_t cb, void *obj)
{
	int fd;
	ucs_status_t status;
	_my_type = SLURM_UCX_SRV;
	slurm_ucx_init();

	/* save server addr to file */
	slurm_ucx_save_server_address();

	status = ucp_worker_get_efd(ucp_worker, &fd);
	if (status != UCS_OK) {
	    return SLURM_ERROR;
	}
	return fd;
}

int slurm_ucx_bind(int fd, slurm_ucx_address_t addr)
{
	int i, rc;

	for(i=0; i < _TBL_MAX; i++){
		if( _conns[i].in_use && (_conns[i].fd == fd) ){
			break;
		}
	}

	xassert( i < _TBL_MAX );

	switch( addr.type ){
	case SLURM_UCX_CLI:
		rc = _connect_to_cli(&addr);
		break;
	case SLURM_UCX_SRV:
		rc = _connect_to_srv(addr.addr.hname);
		break;
	}

	if( SLURM_ERROR == rc ){
		return rc;
	}
	_conns[i].ep_idx = rc;
	return SLURM_SUCCESS;
}

/*
void *_ucx_cli_progress_cb(void *obj)
{

}
*/

int slurm_ucx_init_client()
{
/*
	pthread_attr_t attr_agent;

	_my_type = SLURM_UCX_CLI;
	slurm_ucx_init();

	slurm_attr_init(&attr_agent);
	if (pthread_attr_setdetachstate
	    (&attr_agent, PTHREAD_CREATE_DETACHED))
		error("pthread_attr_setdetachstate error %m");

	while (pthread_create(&thread_agent, &attr_agent,
			      _ucx_cli_progress, (void *)NULL)) {
		error("pthread_create error %m");
		if (++retries > MAX_RETRIES)
			fatal("Can't create pthread");
		usleep(100000);
	}
	slurm_attr_destroy(&attr_agent);

*/
	return SLURM_SUCCESS;
}

int slurm_ucx_conn_open(slurm_ucx_address_t *addr, slurm_ucx_cli_cb_t cb, void *obj)
{
	int i;

	if( SLURM_UCX_CLI == addr->type ){
		/* for now we are not supposed to
		 * connect to clients
		 */
		return SLURM_ERROR;
	}

	for(i = 0; i < _TBL_MAX; i++){
		if( !_conns[i].in_use ){
			break;
		}
	}

	xassert(i < _TBL_MAX);

	_conns[i].ep_idx = _connect_to_srv(addr->addr.hname);
	if( SLURM_ERROR == _conns[i].ep_idx ){
		return SLURM_ERROR;
	}
	_conns[i].in_use = 1;
	_conns[i].fd = socket(AF_UNIX, SOCK_STREAM, 0);
	_conns[i].tag = _conns[i].fd;
	_conns[i].obj = obj;
	_conns[i].cb = cb;
	return _conns[i].fd;
}

int slurm_ucx_conn_close(int fd)
{
	int i;

	for(i = 0; i < _TBL_MAX; i++){
		if( _conns[i].in_use && (_conns[i].fd == fd) ){
			break;
		}
	}
	xassert(i < _TBL_MAX);
	xassert(_ep_tbl[_conns[i].ep_idx].in_use);

	if( _ep_tbl[_conns[i].ep_idx].sname[0] == 0 ){
		/* temp connection - close it */
		ucp_ep_destroy(_ep_tbl[_conns[i].ep_idx].ep);
		_ep_tbl[_conns[i].ep_idx].in_use = 0;
	}

	close(_conns[i].fd);
	_conns[i].in_use = 0;
	return SLURM_SUCCESS;
}

int _process_resp(int i)
{
	int fd, j;
	fd = _rtbl[i].tag & (~TAG_RESP_FLAG);
	for(j = 0; j < _TBL_MAX; j++){
		if( _conns[j].in_use && (_conns[j].fd == fd) ){
			break;
		}
	}
	xassert( j != _TBL_MAX);
	_conns[j].cb(fd, _rtbl[i].buf, _rtbl[i].size, _conns[j].obj);
	/* it is receiver responsibility to free the buffers */
	_rtbl[i].buf = NULL;
	_rtbl[i].size = 0;
	return SLURM_SUCCESS;
}

int _process_req(int i)
{
	int j;

	for(j = 0; j < _TBL_MAX; j++){
		if( !_conns[j].in_use ){
			break;
		}
	}

	_conns[j].fd = socket(AF_UNIX, SOCK_STREAM, 0);
	_conns[j].tag = _rtbl[i].tag | TAG_RESP_FLAG;
	_conns[j].in_use = 1;

	_conns[j].obj = NULL;
	_conns[j].cb = NULL;

	server_cb(_conns[j].fd, _rtbl[i].buf, _rtbl[i].size, server_obj);
	/* it is receiver responsibility to free the buffers */
	_rtbl[i].buf = NULL;
	_rtbl[i].size = 0;
	return SLURM_SUCCESS;
}

static int _process(int i)
{
	int rc;
	if( _rtbl[i].tag & TAG_RESP_FLAG ){
		rc = _process_resp(i);
	} else {
		/* this is response, create virtual channel */
		rc = _process_req(i);
	}
	 return rc;
}

void slurm_ucx_progress()
{
	if( SLURM_UCX_SRV == _my_type ){
		ucp_tag_recv_info_t info_tag;
		int first_avail = -1, i, prev = 0;
		ucp_tag_message_h msg_tag;

		ucp_worker_progress(ucp_worker);
		msg_tag = ucp_tag_probe_nb(ucp_worker,1, 0xffffffffffffffff, 1, &info_tag);

		/* check if one of the requests was finished */
		for(i=0; i <= _rtbl_last; i++){
			if( !_rtbl[i].in_use ){
				first_avail = i;
				continue;
			}
			if( _rtbl[i].req->completed ){
				_process(i);
				_rtbl[i].in_use = 0;
				 ucp_request_release(_rtbl[i].req);
				 if( i == _rtbl_last ){
					 _rtbl_last = prev;
				 }
			} else {
				prev = i;
			}
		}

		xassert(_rtbl_last < _TBL_MAX);
		if( first_avail < 0 ){
			first_avail = _rtbl_last + 1;
		}

		if( NULL != msg_tag ){
			if( first_avail > _rtbl_last ){
				_rtbl_last = first_avail;
			}
			_rtbl[first_avail].in_use = 1;
			_rtbl[first_avail].tag = info_tag.sender_tag;
			_rtbl[first_avail].size = info_tag.length;
			_rtbl[first_avail].buf = xmalloc(_rtbl[first_avail].size);
			_rtbl[first_avail].req = ucp_tag_msg_recv_nb(ucp_worker,
						    (void*)_rtbl[first_avail].buf, _rtbl[first_avail].size,
						    ucp_dt_make_contig(1), msg_tag, recv_handle);
		}
	}
}

void slurm_ucx_poll_prep()
{
	ucs_status_t status = UCS_ERR_BUSY;

	while( status == UCS_ERR_BUSY ){
		status = ucp_worker_arm(ucp_worker);
		if (status == UCS_ERR_BUSY) { /* some events are arrived already */
			slurm_ucx_progress();
			continue;
		}
		xassert( status == UCS_OK);
	}
}

void slurm_ucx_send(int fd, void *buf, size_t size)
{
	int j, k, k_prev = 0;

	for(j = 0; j < _TBL_MAX; j++){
		if( _conns[j].in_use && _conns[j].fd == fd ){
			break;
		}
	}

	xassert( j < _TBL_MAX );

	for(k=0; k <= _stbl_last; k++){
		if( !_stbl[k].in_use ){
			break;
		} else if( _stbl[k].req->completed ){
			xfree(_stbl[k].buf);
			_stbl[k].in_use = 0;
			ucp_request_release(_stbl[k].req);
			if( _stbl_last == k ) {
				_stbl_last = k_prev;
			}
			break;
		} else {
			k_prev = k;
		}
	}
	if( k > _stbl_last ){
		_stbl_last = k;
	}

	xassert( _conns[j].ep_idx );
	_stbl[k].in_use = 1;
	_stbl[k].buf = buf;
	_stbl[k].size = size;
	_stbl[k].tag = _conns[j].tag;
	_stbl[k].req = ucp_tag_send_nb(_ep_tbl[_conns[j].ep_idx].ep, buf, size,
				ucp_dt_make_contig(1), _stbl[k].tag,
				send_handle);
}
