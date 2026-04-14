add_library(host_params INTERFACE)
target_include_directories(host_params INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(host_params INTERFACE cxx_std_17)
