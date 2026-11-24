#pragma once

// Enum reflection for the grpc_utils enumerations, built on magic_enum
// (https://github.com/Neargye/magic_enum): names, parsing, counts and indexes,
// e.g. for readable test output or per-result histograms. Test-framework independent.

#include <grpc_utils/AsyncCallBase.h>
#include <grpc_utils/AsyncWriteStream.h>

#include <magic_enum/magic_enum.hpp>

#include <concepts>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace grpc_test_utils
{

// The public enumerations of grpc_utils
template<typename Enum>
concept LibraryEnum = std::same_as<Enum, grpc_utils::SendResult> || std::same_as<Enum, grpc_utils::WriteOverflowPolicy> ||
                      std::same_as<Enum, grpc_utils::StreamState>;

// Position of an enumerator in magic_enum::enum_values<Enum>(), e.g. to index a histogram
template<LibraryEnum Enum>
constexpr std::size_t enumIndex(Enum value)
{
    const std::optional<std::size_t> index = magic_enum::enum_index(value);
    if (!index)
        throw std::invalid_argument("not an enumerator");
    return *index;
}

// Name of an enumerator, or "<invalid enumerator>" for any other value
template<LibraryEnum Enum>
std::string enumName(Enum value)
{
    const std::string_view name = magic_enum::enum_name(value);
    return name.empty() ? std::string("<invalid enumerator>") : std::string(name);
}

// gRPC status code by name with its number, e.g. "DEADLINE_EXCEEDED (4)"
inline std::string statusCodeName(grpc::StatusCode code)
{
    const std::string_view name = magic_enum::enum_name(code);
    return (name.empty() ? std::string("StatusCode") : std::string(name)) + " (" +
           std::to_string(magic_enum::enum_integer(code)) + ")";
}

// magic_enum reflects only values within MAGIC_ENUM_RANGE_MIN..MAX: make sure it sees every enumerator
static_assert(magic_enum::enum_count<grpc_utils::SendResult>() == 5);
static_assert(magic_enum::enum_count<grpc_utils::WriteOverflowPolicy>() == 2);
static_assert(magic_enum::enum_count<grpc_utils::StreamState>() == 5);

} // namespace grpc_test_utils
