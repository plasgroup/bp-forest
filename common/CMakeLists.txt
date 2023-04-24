add_library(common INTERFACE)
target_include_directories(common INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)

target_link_libraries(common INTERFACE)