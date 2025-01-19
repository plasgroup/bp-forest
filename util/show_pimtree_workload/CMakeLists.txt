add_executable(show_pimtree_workload
    main.cpp
)
target_compile_definitions(show_pimtree_workload PRIVATE $<$<CONFIG:Debug>:DEBUG>)
target_link_libraries(show_pimtree_workload PRIVATE workload_mgmt)
