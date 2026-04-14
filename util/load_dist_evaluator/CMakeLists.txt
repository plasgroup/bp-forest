add_library(load_dist_evaluator INTERFACE)
target_include_directories(load_dist_evaluator INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(load_dist_evaluator INTERFACE cxx_std_17)
target_link_libraries(load_dist_evaluator INTERFACE
    util
    parallel_manager
)
