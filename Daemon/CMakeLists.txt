cmake_minimum_required(VERSION 3.13)
project(Daemon C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

set(COMPILER_FLAGS -Wall -Wextra -fsanitize=address,undefined)

# Shared by both the daemon and anything linking against the client library.
set(COMMON_SOURCES ${PROJECT_SOURCE_DIR}/src/config.c
                    ${PROJECT_SOURCE_DIR}/src/ipc.c)

add_library(daemonclient STATIC
    ${COMMON_SOURCES}
    ${PROJECT_SOURCE_DIR}/src/client.c
)
target_include_directories(daemonclient PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_compile_options(daemonclient PRIVATE ${COMPILER_FLAGS})

add_executable(daemond
    ${COMMON_SOURCES}
    ${PROJECT_SOURCE_DIR}/src/util.c
    ${PROJECT_SOURCE_DIR}/src/state.c
    ${PROJECT_SOURCE_DIR}/src/process.c
    ${PROJECT_SOURCE_DIR}/src/ipc_server.c
    ${PROJECT_SOURCE_DIR}/src/main.c
)
target_include_directories(daemond PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_compile_options(daemond PRIVATE ${COMPILER_FLAGS})
target_link_options(daemond PRIVATE ${COMPILER_FLAGS})

add_executable(demo_worker
    ${PROJECT_SOURCE_DIR}/src/demo_worker.c
)
target_link_libraries(demo_worker PRIVATE daemonclient)
target_compile_options(demo_worker PRIVATE ${COMPILER_FLAGS})
target_link_options(demo_worker PRIVATE ${COMPILER_FLAGS})

# Dummy payload application: talks to the "network" bridge process
# (Network/src/main.c) over the daemon IPC bus using the net_ipc_hdr_t
# framing shared with it - hence the extra include dir.
add_executable(payload_app
    ${PROJECT_SOURCE_DIR}/src/payload_app.c
)
target_link_libraries(payload_app PRIVATE daemonclient)
target_include_directories(payload_app PRIVATE ${PROJECT_SOURCE_DIR}/../Network/include)
target_compile_options(payload_app PRIVATE ${COMPILER_FLAGS})
target_link_options(payload_app PRIVATE ${COMPILER_FLAGS})
