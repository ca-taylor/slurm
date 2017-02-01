#include <ucp/api/ucp.h>
#include <unistd.h>

#include "src/common/ucx.h"


int slurm_ucx_addr(slurm_ucx_address_t *addr)
{
	int fd[2];
	pipe(fd);
	return fd[0];
}

slurm_ucx_type_t slurm_ucx_whoami()
{
	return 0;
}

int slurm_ucx_reachable(slurm_ucx_address_t *addr)
{
	return 0;
}

int slurm_ucx_init_server(char *fname, slurm_ucx_srv_cb_t cb, void *obj)
{
	return 0;
}

int slurm_ucx_bind(int fd, slurm_ucx_address_t addr)
{
	return 0;
}

int slurm_ucx_init_client()
{
	return 0;
}

int slurm_ucx_conn(slurm_ucx_address_t *addr, slurm_ucx_cli_cb_t cb, void *obj)
{
	return 0;
}

int slurm_ucx_conn_close(int fd)
{
	return 0;
}

void slurm_ucx_progress()
{

}

void slurm_ucx_poll_prep()
{
}

void slurm_ucx_send(int fd, void *buf, size_t size)
{
}
