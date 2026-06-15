# FindNUMA.cmake
# -----------------------------------------------------------------------------
# Find libnuma, the NUMA policy library shipped with the numactl project.
#
# libnuma provides no upstream CMake config file, and its pkg-config file
# (numa.pc) is not installed by every distribution, so this module locates the
# header and library directly.
#
# Result variables:
#   NUMA_FOUND        - True if libnuma was found.
#   NUMA_INCLUDE_DIRS - Include directory that contains numa.h.
#   NUMA_LIBRARIES    - The libnuma library to link against.
#   NUMA_LIBRARY_DIR  - Directory that contains the libnuma library
#                       (informational; prefer the NUMA::NUMA target instead of
#                       feeding this to link_directories()).
#   NUMA_API_VERSION  - libnuma API level read from <numa.h> (e.g. 2), when
#                       detectable. NOTE: this is the API level, *not* the
#                       package/soname version, so it is informational only.
#
# Imported target (preferred):
#   NUMA::NUMA        - Link against this with target_link_libraries(); it also
#                       carries the include directory as a usage requirement.
#
# Hints:
#   NUMA_ROOT (CMake variable or environment variable) - extra search location
#   for a libnuma installed outside the default system prefixes. (CMake also
#   searches CMAKE_PREFIX_PATH and <PackageName>_ROOT automatically.)
#
# If not found, install the development package:
#   Debian/Ubuntu : libnuma-dev
#   RHEL/Fedora   : numactl-devel
#   openSUSE      : libnuma-devel
#   Arch          : numactl
# -----------------------------------------------------------------------------

find_path(NUMA_INCLUDE_DIR
  NAMES numa.h
  HINTS ${NUMA_ROOT} ENV NUMA_ROOT
  PATH_SUFFIXES include
  DOC "libnuma include directory (contains numa.h)")

find_library(NUMA_LIBRARY
  NAMES numa
  HINTS ${NUMA_ROOT} ENV NUMA_ROOT
  PATH_SUFFIXES lib lib64
  DOC "libnuma library (libnuma.so / libnuma.a)")

if(NUMA_LIBRARY)
  get_filename_component(NUMA_LIBRARY_DIR "${NUMA_LIBRARY}" DIRECTORY)
endif()

# Best-effort: read the API-level macro from numa.h. It is absent in very old
# libnuma versions, so it must never be treated as a required result.
if(NUMA_INCLUDE_DIR AND EXISTS "${NUMA_INCLUDE_DIR}/numa.h")
  file(STRINGS "${NUMA_INCLUDE_DIR}/numa.h" _numa_api_line
    REGEX "^[ \t]*#[ \t]*define[ \t]+LIBNUMA_API_VERSION[ \t]+[0-9]+")
  if(_numa_api_line)
    string(REGEX REPLACE
      ".*LIBNUMA_API_VERSION[ \t]+([0-9]+).*" "\\1"
      NUMA_API_VERSION "${_numa_api_line}")
  endif()
  unset(_numa_api_line)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NUMA
  REQUIRED_VARS NUMA_LIBRARY NUMA_INCLUDE_DIR)

if(NUMA_FOUND)
  set(NUMA_LIBRARIES    "${NUMA_LIBRARY}")
  set(NUMA_INCLUDE_DIRS "${NUMA_INCLUDE_DIR}")

  if(NOT TARGET NUMA::NUMA)
    add_library(NUMA::NUMA UNKNOWN IMPORTED)
    set_target_properties(NUMA::NUMA PROPERTIES
      IMPORTED_LOCATION                 "${NUMA_LIBRARY}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "C"
      INTERFACE_INCLUDE_DIRECTORIES     "${NUMA_INCLUDE_DIR}")
  endif()
endif()

mark_as_advanced(NUMA_INCLUDE_DIR NUMA_LIBRARY NUMA_LIBRARY_DIR)
