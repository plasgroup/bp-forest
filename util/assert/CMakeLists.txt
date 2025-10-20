add_library(assert INTERFACE)
target_include_directories(assert INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(assert INTERFACE cxx_std_17)
