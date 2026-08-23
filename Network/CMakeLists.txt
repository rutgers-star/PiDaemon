cmake_minimum_required(VERSION 3.13)
project(Communications)

SET(COMPILER_FLAGS "-fsanitize=address,undefined")

set(PROJECT_SOURCES     ${PROJECT_SOURCE_DIR}/src/main.c)

add_executable(Communications ${PROJECT_SOURCES})

target_include_directories(Communications PUBLIC ${PROJECT_BINARY_DIR} ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(Communications PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/lib/spicenet/libspicenet.a m daemonclient)
 
target_compile_options(${PROJECT_NAME} PRIVATE ${COMPILER_FLAGS})
target_link_options(${PROJECT_NAME} PRIVATE ${COMPILER_FLAGS})  