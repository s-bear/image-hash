# CMakeList.txt : CMake project for imghash, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project (imghash)

find_package(JPEG)
find_package(PNG)


# Add source to this project's executable.
add_executable (imghash main.cpp imghash.cpp imghash.h)

target_compile_features(imghash PUBLIC cxx_std_17)

if(JPEG_FOUND)
target_sources(imghash PUBLIC jpeg.cpp)
target_include_directories(imghash PRIVATE ${JPEG_INCLUDE_DIR})
target_link_libraries(imghash PRIVATE ${JPEG_LIBRARIES})
target_compile_definitions(imghash PRIVATE USE_JPEG _CRT_SECURE_NO_WARNINGS)
endif()

if(PNG_FOUND)
target_sources(imghash PUBLIC png.cpp)
target_link_libraries(imghash PRIVATE PNG::PNG)
target_compile_definitions(imghash PRIVATE USE_PNG)
endif()


# TODO: Add tests and install targets if needed.
