#pragma once

// Catch2 string makers: grpc_utils enumerations and grpc::StatusCode are shown
// by name in Catch2 assertion messages ("Dropped", "DEADLINE_EXCEEDED (4)")
// instead of bare numbers. Include in Catch2 test sources; requires Catch2 v3
// and magic_enum (via EnumReflection.h).

#include <grpc_test_utils/EnumReflection.h>

#include <catch2/catch_tostring.hpp>

#include <string>

namespace grpc_test_utils::detail
{

template<LibraryEnum Enum>
struct EnumStringMaker
{
    static std::string convert(Enum value) { return enumName(value); }
};

} // namespace grpc_test_utils::detail

template<>
struct Catch::StringMaker<grpc_utils::SendResult> : grpc_test_utils::detail::EnumStringMaker<grpc_utils::SendResult>
{
};

template<>
struct Catch::StringMaker<grpc_utils::WriteOverflowPolicy>
    : grpc_test_utils::detail::EnumStringMaker<grpc_utils::WriteOverflowPolicy>
{
};

template<>
struct Catch::StringMaker<grpc_utils::StreamState> : grpc_test_utils::detail::EnumStringMaker<grpc_utils::StreamState>
{
};

template<>
struct Catch::StringMaker<grpc::StatusCode>
{
    static std::string convert(grpc::StatusCode code) { return grpc_test_utils::statusCodeName(code); }
};
