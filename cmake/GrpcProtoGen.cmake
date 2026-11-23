# grpc_async_stream_add_proto_library(<target> PROTOS <file.proto>...)
#
# Generates protobuf + gRPC C++ sources for the given .proto files with protoc
# and grpc_cpp_plugin, and builds them into a static library <target> that
# exposes the generated headers. Works both with an installed gRPC and with
# gRPC built from source as a subproject (see Dependencies.cmake).

include_guard(GLOBAL)

function(grpc_async_stream_add_proto_library target)
    cmake_parse_arguments(ARG "" "" "PROTOS" ${ARGN})
    if(NOT ARG_PROTOS)
        message(FATAL_ERROR "grpc_async_stream_add_proto_library: no PROTOS given")
    endif()
    if(NOT GrpcAsyncStream_protoc OR NOT GrpcAsyncStream_grpc_cpp_plugin)
        message(FATAL_ERROR "grpc_async_stream_add_proto_library: protoc or grpc_cpp_plugin not available")
    endif()

    set(out_dir "${CMAKE_CURRENT_BINARY_DIR}/${target}_generated")
    file(MAKE_DIRECTORY "${out_dir}")

    set(generated_sources)
    foreach(proto IN LISTS ARG_PROTOS)
        get_filename_component(proto_abs "${proto}" ABSOLUTE)
        get_filename_component(proto_dir "${proto_abs}" DIRECTORY)
        get_filename_component(proto_name "${proto_abs}" NAME_WE)

        set(outputs
            "${out_dir}/${proto_name}.pb.cc"
            "${out_dir}/${proto_name}.pb.h"
            "${out_dir}/${proto_name}.grpc.pb.cc"
            "${out_dir}/${proto_name}.grpc.pb.h")

        add_custom_command(
            OUTPUT ${outputs}
            COMMAND ${GrpcAsyncStream_protoc}
                --proto_path=${proto_dir}
                --cpp_out=${out_dir}
                --grpc_out=${out_dir}
                --plugin=protoc-gen-grpc=${GrpcAsyncStream_grpc_cpp_plugin}
                ${proto_abs}
            DEPENDS ${proto_abs} ${GrpcAsyncStream_protoc_target} ${GrpcAsyncStream_grpc_cpp_plugin_target}
            COMMENT "Generating protobuf/gRPC sources for ${proto_name}.proto"
            VERBATIM)

        list(APPEND generated_sources ${outputs})
    endforeach()

    add_library(${target} STATIC ${generated_sources})
    target_include_directories(${target} SYSTEM PUBLIC "${out_dir}")
    target_link_libraries(${target} PUBLIC gRPC::grpc++ protobuf::libprotobuf)
    target_compile_features(${target} PUBLIC cxx_std_17)
endfunction()
