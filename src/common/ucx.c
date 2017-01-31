#include <ucp/api/ucp.h>

#include "src/common/ucx.h"

void ucp_version(unsigned *major, unsigned *minor, unsigned *release) {
    ucp_get_version(major, minor, release);
}
