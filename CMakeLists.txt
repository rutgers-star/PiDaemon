cmake_minimum_required(VERSION 3.13)
project(PiObcCommunications C)

# Builds daemond/daemonclient/demo_worker/payload_app, then the Network
# bridge (Communications), which links against daemonclient to talk to
# payload_app over the daemon's IPC bus.
add_subdirectory(Daemon)
add_subdirectory(Network)

# daemond supervises Communications like any other managed process (see
# register_network() in Daemon/src/main.c), so it needs to know where the
# real build actually put the binary. Only available in this combined
# build - a standalone `cd Daemon && cmake ..` build leaves NETWORK_BIN_PATH
# undefined and register_network() just skips registering it.
if(TARGET daemond AND TARGET Communications)
    target_compile_definitions(daemond PRIVATE NETWORK_BIN_PATH="$<TARGET_FILE:Communications>")
endif()
