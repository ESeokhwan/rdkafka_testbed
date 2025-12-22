cmake_minimum_required(VERSION 3.14)
project(boost_testbed)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 11)

add_compile_options(-Wall -Wextra -O2)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
find_package(Threads REQUIRED)

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--disable-new-dtags")
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_RPATH "${PROJECT_SOURCE_DIR}/librdkafka/install/lib;${PROJECT_SOURCE_DIR}/libmoniq/install/lib")

include_directories(
    ${PROJECT_SOURCE_DIR}/librdkafka/install/include
    ${PROJECT_SOURCE_DIR}/libmoniq/install/include
    ${PROJECT_SOURCE_DIR}/src/common
)
link_directories(
    ${PROJECT_SOURCE_DIR}/librdkafka/install/lib
    ${PROJECT_SOURCE_DIR}/libmoniq/install/lib
)

file(GLOB_RECURSE COMMON_SOURCES
    "${PROJECT_SOURCE_DIR}/src/common/*.c"
    "${PROJECT_SOURCE_DIR}/src/common/*.cpp"
)
file(GLOB_RECURSE COMMON_HEADERS
    "${PROJECT_SOURCE_DIR}/src/common/*.h"
    "${PROJECT_SOURCE_DIR}/src/common/*.hpp"
)
add_library(common STATIC
    ${COMMON_SOURCES}
    ${COMMON_HEADERS}
)

target_include_directories(common PUBLIC
    ${PROJECT_SOURCE_DIR}/src/common
)

file(GLOB APP_SOURCES
    "${PROJECT_SOURCE_DIR}/src/apps/*.c"
    "${PROJECT_SOURCE_DIR}/src/apps/*.cpp"
)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)
foreach(source_file ${APP_SOURCES})
    get_filename_component(exe_name ${source_file} NAME_WE)
    add_executable(${exe_name} ${source_file})
    target_link_libraries(${exe_name} PRIVATE
        common moniq rdkafka++ rdkafka Threads::Threads
    )
endforeach()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/sandbox")
    add_subdirectory(sandbox EXCLUDE_FROM_ALL)
endif()

