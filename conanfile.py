"""Conan 2 recipe providing the dependencies of GrpcAsyncStream's build.

The CMake project uses dependencies that are already imported or found as packages
before falling back to building them with CPM (see cmake/Dependencies.cmake), so
with this recipe gRPC, protobuf, magic_enum and Catch2 all come from Conan:

    conan install . --build=missing -s compiler.cppstd=20
    cmake --preset conan-release
    cmake --build --preset conan-release
    ctest --preset conan-release -LE benchmark

Options:
    with_tests       build this project's tests and benchmarks (requires Catch2)
    with_test_utils  provide the GrpcAsyncStream::TestUtils target (requires magic_enum)
"""

from conan import ConanFile
from conan.tools.build import check_min_cppstd, cross_building
from conan.tools.cmake import CMakeDeps, CMakeToolchain, cmake_layout


class GrpcAsyncStreamConan(ConanFile):
    name = "grpc-async-stream"
    version = "2.0.0"
    license = "MIT"
    description = "Header-only C++20 wrappers over the gRPC asynchronous client-streaming and bidirectional client streams"
    url = "https://github.com/egorodet/GrpcAsyncStream"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "with_tests": [True, False],
        "with_test_utils": [True, False],
    }
    default_options = {
        "with_tests": True,
        "with_test_utils": True,
        # Only the C++ code generator of gRPC is needed (for the test protocol)
        "grpc/*:codegen": True,
        "grpc/*:cpp_plugin": True,
        "grpc/*:csharp_plugin": False,
        "grpc/*:node_plugin": False,
        "grpc/*:objective_c_plugin": False,
        "grpc/*:php_plugin": False,
        "grpc/*:python_plugin": False,
        "grpc/*:ruby_plugin": False,
    }

    def requirements(self):
        self.requires("grpc/1.72.0")
        if self.options.with_tests or self.options.with_test_utils:
            self.requires("magic_enum/0.9.8")
        if self.options.with_tests:
            self.requires("catch2/3.8.1")

    def build_requirements(self):
        # protoc and grpc_cpp_plugin generate the test protocol. In native builds they
        # come with the host packages; when cross-building they must run on the build
        # machine, so they are tool requirements (put on PATH by the build environment).
        if self.options.with_tests and cross_building(self):
            self.tool_requires("protobuf/<host_version>")
            self.tool_requires("grpc/<host_version>")

    def validate(self):
        # The library is C++20, and Abseil's ABI depends on the C++ standard, so gRPC,
        # protobuf and Abseil must be built with the same standard: pass
        # -s compiler.cppstd=20 (or gnu20), which applies to every package.
        check_min_cppstd(self, 20)

    def layout(self):
        cmake_layout(self)

    def generate(self):
        toolchain = CMakeToolchain(self)
        # Dependencies come from Conan: fail instead of silently building gRPC with CPM
        toolchain.cache_variables["GRPC_ASYNC_STREAM_GRPC_PROVIDER"] = "package"
        toolchain.cache_variables["GRPC_ASYNC_STREAM_BUILD_TESTS"] = bool(self.options.with_tests)
        toolchain.cache_variables["GRPC_ASYNC_STREAM_BUILD_BENCHMARKS"] = bool(self.options.with_tests)
        toolchain.cache_variables["GRPC_ASYNC_STREAM_BUILD_TEST_UTILS"] = bool(self.options.with_test_utils)
        toolchain.generate()

        CMakeDeps(self).generate()
