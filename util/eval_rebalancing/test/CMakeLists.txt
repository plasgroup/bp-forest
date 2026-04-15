add_executable(collect_all_data_test
    collect_all_data_test.cpp
)
target_include_directories(collect_all_data_test PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/../src
)
target_link_libraries(collect_all_data_test
    common_base
)
target_compile_features(collect_all_data_test PRIVATE cxx_std_17)
