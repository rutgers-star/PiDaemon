cmake_minimum_required(VERSION 3.13)
project(Obc C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

set(COMPILER_FLAGS -Wall -Wextra -fsanitize=address,undefined)

# Standalone busOBC-side harness: owns the RS-422 link directly (no
# daemon/IPC layer - unlike Network/, this isn't running alongside daemond
# on the same box) and only needs the shared APID registry + wire framing.
add_executable(obc_daemon ${PROJECT_SOURCE_DIR}/src/main.c)
target_link_libraries(obc_daemon PRIVATE STAR::payload_apids payload_link m)
target_compile_options(obc_daemon PRIVATE ${COMPILER_FLAGS})
target_link_options(obc_daemon PRIVATE ${COMPILER_FLAGS})
