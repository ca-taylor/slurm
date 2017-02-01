#ifndef _SLURM_UCX_H_
#define _SLURM_UCX_H_

#include <stdint.h>


typedef enum {
	SLURM_UCX_SRV,
	SLURM_UCX_CLI
} slurm_ucx_type_t;

typedef struct {
	slurm_ucx_type_t type;
	union {
		char *hname;
		struct {
			char *data;
			size_t len;
		} address;
	} addr;
} slurm_ucx_address_t;

/* get UCX address
 * for the serer it returns hostname
 * for the client it will be an address obtained from UCX
 */
int slurm_ucx_addr(slurm_ucx_address_t *addr);

/* get this process UCX role */
slurm_ucx_type_t slurm_ucx_whoami();

/*
 * Verify that we can use UCX for the target
 * Always return false if UCX wasn't initialized
 */
int slurm_ucx_reachable(slurm_ucx_address_t *addr);


/*
 * fd - newly created virtual connection descriptor
 * buf, size - received message
 * obj - smth user provided to us, will be NULL I guess
 * NOTE: fd doesn't correspond to a certain EP at the time of this call as
 * the sender is unknown. Once message is processed EP will be initialized and added
 * using slurm_ucx_bind(fd, slurm_ucx_address_t addr);
 * also the tag of the initiation message has to be stored internally
 * so the response will reach correct receiver on the EP.
 */
typedef void (*slurm_ucx_srv_cb_t)(int fd, void *buf, size_t size, void *obj);

/*
 * fd - file descriptor that will be used for polling
 * cb - a callback that UCX module will call when message is received
 * obj - arbitrary data that callback may need to use
 */
int slurm_ucx_init_server(char *fname, slurm_ucx_srv_cb_t cb, void *obj);

/*
 * Map fd with an address (on the server side)
 */
int slurm_ucx_bind(int fd, slurm_ucx_address_t addr);

/*
 * Initialize UCX component for the client
 * Incurs a separate progress thread creation to call
 * ucx progress and calling registered callbacks
 */
int slurm_ucx_init_client();


/*
 * client-side receiv callback
 */
typedef void (*slurm_ucx_cli_cb_t)(int fd, void *buf, size_t size, void *obj);

/*
 * open the virtual connection to a remote server
 * addr - remote endpoint address
 *
 */
int slurm_ucx_conn(slurm_ucx_address_t *addr, slurm_ucx_cli_cb_t cb, void *obj);

/*
 * close the virtual connection to a remote server
 * addr - remote endpoint address
 *
 */
int slurm_ucx_conn_close(int fd);

/*
 * progress both client and server
 */
void slurm_ucx_progress();

/*
 * Prepare server for polling.
 *
 */
void slurm_ucx_poll_prep();

/*
 * Prepare server for polling.
 *
 */
void slurm_ucx_send(int fd, void *buf, size_t size);


#endif
