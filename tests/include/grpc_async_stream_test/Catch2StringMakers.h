// Copyright (c) 2026 Evgeny Gorodetskiy
// SPDX-License-Identifier: MIT
// Licensed under the MIT License. See the LICENSE file in the project root for details.

#pragma once

// Catch2 shows the library enums and grpc::StatusCode by name ("Dropped", "DEADLINE_EXCEEDED (4)").
// Requires Catch2 v3 and magic_enum.

#include <grpc_async_stream_test/EnumReflection.h>

#include <catch2/catch_tostring.hpp>

#include <string>

namespace grpc_async_stream_test::detail
{

template<LibraryEnum Enum>
struct EnumStringMaker
{
    static std::string convert(Enum value) { return enumName(value); }
};

} // namespace grpc_async_stream_test::detail

template<>
struct Catch::StringMaker<grpc_async_stream::SendResult>
    : grpc_async_stream_test::detail::EnumStringMaker<grpc_async_stream::SendResult>
{
};

template<>
struct Catch::StringMaker<grpc_async_stream::WriteOverflowPolicy>
    : grpc_async_stream_test::detail::EnumStringMaker<grpc_async_stream::WriteOverflowPolicy>
{
};

template<>
struct Catch::StringMaker<grpc_async_stream::StreamState>
    : grpc_async_stream_test::detail::EnumStringMaker<grpc_async_stream::StreamState>
{
};

template<>
struct Catch::StringMaker<grpc::StatusCode>
{
    static std::string convert(grpc::StatusCode code) { return grpc_async_stream_test::statusCodeName(code); }
};
