add_library(parallel_manager INTERFACE)
target_include_directories(parallel_manager INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(parallel_manager INTERFACE cxx_std_17)

find_package(Threads REQUIRED)
target_link_libraries(parallel_manager INTERFACE Threads::Threads)
