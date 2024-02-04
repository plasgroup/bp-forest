add_library(cmdline INTERFACE)
target_include_directories(cmdline INTERFACE ${CMAKE_CURRENT_LIST_DIR}/cmdline)

set(BUILD_DOC OFF CACHE BOOL "skip building documents of cereal")
set(BUILD_SANDBOX OFF CACHE BOOL "skip building examples of cereal")
set(SKIP_PERFORMANCE_COMPARISON ON CACHE BOOL "skip performance measurement for cereal")
add_subdirectory(cereal)
