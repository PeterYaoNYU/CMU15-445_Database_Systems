add_subdirectory(buffer)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(concurrency)
add_subdirectory(container)
add_subdirectory(execution)
add_subdirectory(recovery)
add_subdirectory(storage)
add_subdirectory(type)

add_library(bustub SHARED ${ALL_OBJECT_FILES})

set(BUSTUB_LIBS
    bustub_buffer 
    bustub_catalog 
    bustub_common 
    bustub_concurrency 
    bustub_execution 
    bustub_recovery 
    bustub_type
    bustub_container_hash
    bustub_storage_disk
    bustub_storage_index
    bustub_storage_page
    bustub_storage_table
)

set(BUSTUB_THIRDPARTY_LIBS
    bustub_murmur3)

target_link_libraries(
    bustub 
    ${BUSTUB_LIBS}
    ${BUSTUB_THIRDPARTY_LIBS})

target_include_directories(
bustub PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
