# Copyright (C) 2026 Zilliz. All rights reserved.
# Licensed under the Apache License, Version 2.0.

# Keep the dependency commit fixed and apply the additive telemetry ABI patch.
# Never reset or overwrite unrelated edits in an existing dependency checkout.
find_package(Git REQUIRED)
set(_patch "${CMAKE_CURRENT_LIST_DIR}/patches/manifest-read-metrics.patch")
execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${STORAGE_SOURCE_DIR}" apply --reverse --check "${_patch}"
    RESULT_VARIABLE _already_applied
    OUTPUT_QUIET ERROR_QUIET)
if(NOT _already_applied EQUAL 0)
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${STORAGE_SOURCE_DIR}" apply --check "${_patch}"
        RESULT_VARIABLE _check
        ERROR_VARIABLE _error)
    if(NOT _check EQUAL 0)
        message(FATAL_ERROR "milvus-storage telemetry patch does not match this checkout: ${_error}")
    endif()
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" -C "${STORAGE_SOURCE_DIR}" apply "${_patch}"
        RESULT_VARIABLE _apply
        ERROR_VARIABLE _error)
    if(NOT _apply EQUAL 0)
        message(FATAL_ERROR "Cannot apply milvus-storage telemetry patch: ${_error}")
    endif()
endif()
