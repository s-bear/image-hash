# CMakeList.txt : CMake project for imghash, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

cmake_policy(SET CMP0091 NEW)
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
set(VCPKG_TARGET_TRIPLET x64-windows-static)

project (imghash)

find_package(JPEG)
find_package(PNG)
find_package(SQLiteCpp)

# Add source to this project's executable.
add_executable (imghash main.cpp imghash.cpp)

target_compile_features(imghash PUBLIC cxx_std_17)

if(JPEG_FOUND)
target_sources(imghash PUBLIC jpeg.cpp)
target_include_directories(imghash PRIVATE ${JPEG_INCLUDE_DIR})
target_link_libraries(imghash PRIVATE ${JPEG_LIBRARIES})
target_compile_definitions(imghash PRIVATE USE_JPEG _CRT_SECURE_NO_WARNINGS)
set(features "${features} JPEG")
endif()

if(PNG_FOUND)
target_sources(imghash PUBLIC png.cpp)
target_link_libraries(imghash PRIVATE PNG::PNG)
target_compile_definitions(imghash PRIVATE USE_PNG)
set(features "${features} PNG")
endif()

if(SQLiteCpp_FOUND)
target_sources(imghash PUBLIC mvptable.cpp db.cpp)
target_link_libraries(imghash PRIVATE SQLiteCpp)
target_compile_definitions(imghash PRIVATE USE_SQLITE)
set(features "${features} SQLITE")
endif()

message(STATUS "Compiling with features: ${features}")
# TODO: Add tests and install targets if needed.
