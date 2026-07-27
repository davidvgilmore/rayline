cmake_minimum_required(VERSION 3.20)
project(rayline-c82-encoder LANGUAGES C CXX)

if(NOT LLAMA_CPP_SOURCE)
    message(FATAL_ERROR "LLAMA_CPP_SOURCE must point at the pinned llama.cpp checkout")
endif()

set(
    RAYLINE_PINNED_LLAMA_COMMIT
    "d73cd076740db9c111d0e58ddd4486904469e75e"
)
if(NOT RAYLINE_LLAMA_COMMIT)
    set(RAYLINE_LLAMA_COMMIT "${RAYLINE_PINNED_LLAMA_COMMIT}")
endif()
if(NOT RAYLINE_LLAMA_COMMIT STREQUAL RAYLINE_PINNED_LLAMA_COMMIT)
    message(
        FATAL_ERROR
        "RAYLINE_LLAMA_COMMIT must be the audited upstream commit ${RAYLINE_PINNED_LLAMA_COMMIT}"
    )
endif()
find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${LLAMA_CPP_SOURCE}/.git")
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${LLAMA_CPP_SOURCE}" rev-parse HEAD
        OUTPUT_VARIABLE LLAMA_CPP_HEAD
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE LLAMA_CPP_GIT_RESULT
    )
    if(NOT LLAMA_CPP_GIT_RESULT EQUAL 0 OR NOT LLAMA_CPP_HEAD STREQUAL RAYLINE_PINNED_LLAMA_COMMIT)
        message(
            FATAL_ERROR
            "LLAMA_CPP_SOURCE must be upstream llama.cpp at ${RAYLINE_PINNED_LLAMA_COMMIT}; got ${LLAMA_CPP_HEAD}"
        )
    endif()
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_COMMON OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_SERVER OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_APP OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_UI OFF CACHE BOOL "" FORCE)
set(GGML_METAL_EMBED_LIBRARY ON CACHE BOOL "" FORCE)

add_subdirectory("${LLAMA_CPP_SOURCE}" llama.cpp EXCLUDE_FROM_ALL)

add_executable(
    rayline-c82-encoder
    encoder.cpp
    main.cpp
    token_blocks.cpp
)
target_link_libraries(rayline-c82-encoder PRIVATE llama)
target_include_directories(
    rayline-c82-encoder
    PRIVATE
        "${LLAMA_CPP_SOURCE}/include"
        "${LLAMA_CPP_SOURCE}/ggml/include"
        "${LLAMA_CPP_SOURCE}/vendor"
)
target_compile_definitions(
    rayline-c82-encoder
    PRIVATE
        RAYLINE_LLAMA_COMMIT="${RAYLINE_LLAMA_COMMIT}"
)
target_compile_options(
    rayline-c82-encoder
    PRIVATE
        $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-Wpedantic>
)
