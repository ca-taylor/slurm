##*****************************************************************************
#  AUTHOR:
#    Artem Polyakov <artpol84@gmail.com>
#
#  SYNOPSIS:
#    X_AC_PMIX
#
#  DESCRIPTION:
#    Determine if the PMIx libraries exists. Derived from "x_ac_hwloc.m4".
##*****************************************************************************

AC_DEFUN([X_AC_PMIX],
[
  _x_ac_pmix_dirs="/usr /usr/local"
  _x_ac_pmix_libs="lib64 lib"

  AC_ARG_WITH(
    [pmix],
    AS_HELP_STRING(--with-pmix=PATH,Specify path to pmix installation),
    [_x_ac_pmix_dirs="$withval $_x_ac_pmix_dirs"])

  AC_CACHE_CHECK(
    [for pmix installation],
    [x_ac_cv_pmix_dir],
    [
      for d in $_x_ac_pmix_dirs; do
        test -d "$d" || continue
        test -d "$d/include" || continue
        test -f "$d/include/pmix_common.h" || continue
        test -f "$d/include/pmix_server.h" || continue
        for libdir in $_x_ac_pmix_libs; do
          test -d "$d/$libdir" || continue
          _x_ac_pmix_cppflags_save="$CPPFLAGS"
          CPPFLAGS="-I$d/include $CPPFLAGS"
          _x_ac_pmix_libs_save="$LIBS"
          LIBS="-L$d/$libdir -lpmix-server $LIBS"
          AC_LINK_IFELSE(
            [AC_LANG_CALL([], PMIx_Get_version)],
            AS_VAR_SET(x_ac_cv_pmix_dir, $d))
          CPPFLAGS="$_x_ac_pmix_cppflags_save"
          LIBS="$_x_ac_pmix_libs_save"
          test -n "$x_ac_cv_pmix_dir" && break
        done
        test -n "$x_ac_cv_pmix_dir" && break
      done
    ])

  if test -z "$x_ac_cv_pmix_dir"; then
    AC_MSG_WARN([unable to locate pmix installation])
  else
    PMIX_CPPFLAGS="-I$x_ac_cv_pmix_dir/include"
    _x_ac_pmix_libdir=""
    for libdir in $_x_ac_pmix_libs; do
      test -d "$d/$libdir" || continue
      _x_ac_pmix_libdir=$libdir
      break
    done
    if test "$ac_with_rpath" = "yes"; then
      PMIX_LDFLAGS="-Wl,-rpath -Wl,$x_ac_cv_pmix_dir/$_x_ac_pmix_libdir -L$x_ac_cv_hwloc_dir/$_x_ac_pmix_libdir"
    else
      PMIX_LDFLAGS="-L$x_ac_cv_hwloc_dir/$_x_ac_pmix_libdir"
    fi
    PMIX_LIBS="-lpmix-server"
    AC_DEFINE(HAVE_PMIX, 1, [Define to 1 if pmix library found])
  fi

  AC_SUBST(PMIX_LIBS)
  AC_SUBST(PMIX_CPPFLAGS)
  AC_SUBST(PMIX_LDFLAGS)
])
