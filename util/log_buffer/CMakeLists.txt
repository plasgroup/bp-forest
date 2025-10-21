add_library(log_buffer INTERFACE)
target_include_directories(log_buffer INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(log_buffer INTERFACE cxx_std_17)
