#

AUTOMAKE_OPTIONS = foreign
CLEANFILES = core.*

AM_CPPFLAGS = -I$(top_srcdir)

bin_PROGRAMS = sbatch

sbatch_SOURCES = sbatch.c opt.c opt.h

convenience_libs = $(top_builddir)/src/api/libslurm.o $(DL_LIBS) $(UCX_LIBS)

sbatch_LDADD = $(convenience_libs)

sbatch_LDFLAGS = -export-dynamic $(CMD_LDFLAGS) $(UCX_LDFLAGS)

force:
$(convenience_libs) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
