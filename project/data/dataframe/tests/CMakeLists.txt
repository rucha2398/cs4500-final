cmake_minimum_required(VERSION 2.8.2)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(Threads REQUIRED)

configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(
    COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download
    )

if (result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} --build .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download
    )

if (result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL
                 )

add_executable(submitted_test_1 submitted_test_1.cpp)
target_link_libraries(submitted_test_1 gtest)

add_executable(submitted_test_2 submitted_test_2.cpp)
target_link_libraries(submitted_test_2 gtest)

add_executable(submitted_test_3 submitted_test_3.cpp)
target_link_libraries(submitted_test_3 gtest)

add_executable(submitted_test_4 submitted_test_4.cpp)
target_link_libraries(submitted_test_4 gtest)

add_executable(submitted_test_5 submitted_test_5.cpp)
target_link_libraries(submitted_test_5 gtest)

add_executable(submitted_test_6 submitted_test_6.cpp)
target_link_libraries(submitted_test_6 gtest)

add_executable(submitted_test_7 submitted_test_7.cpp)
target_link_libraries(submitted_test_7 gtest)
