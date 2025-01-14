add_executable(show_pimtree_workload
    show_pimtree_workload.cpp
)
target_include_directories(show_pimtree_workload PRIVATE ${CMAKE_SOURCE_DIR})
target_compile_definitions(show_pimtree_workload PRIVATE $<$<CONFIG:Debug>:DEBUG>)
target_link_libraries(show_pimtree_workload PRIVATE workload_mgmt)
