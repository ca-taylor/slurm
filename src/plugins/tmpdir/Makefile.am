# Makefile for TMPDIR SPANK plugin

AUTOMAKE_OPTIONS = foreign

AM_CPPFLAGS = -DDEFAULT_SCRIPT_DIR=\"$(sysconfdir)\" \
	-I$(top_srcdir) -I$(top_srcdir)/src/common

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

# TMPDIR plugin.
pkglib_LTLIBRARIES = tmpdir.la

# TMPDIR plugin.
tmpdir_la_SOURCES = tmpdir.c
tmpdir_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)
