# AGENTS.md

Instructions for AI coding agents and human contributors working on GrpcAsyncStream. The
[README](README.md) is the full reference; this file is the short checklist for making changes.

## Project at a glance

- Header-only C++20 library wrapping gRPC async **client** streams (bidirectional, client-streaming,
  server-streaming) behind a single-threaded, poll-driven API with a bounded write queue.
- Library headers: [`include/grpc_async_stream/`](include/grpc_async_stream) — target
  `GrpcAsyncStream::GrpcAsyncStream`, depends on gRPC only.
- Reusable test utilities: [`tests/include/grpc_async_stream_test/`](tests/include/grpc_async_stream_test) —
  target `GrpcAsyncStream::TestUtils`, adds magic_enum (and Catch2 for `Catch2StringMakers.h` only).
- Tests: `tests/*.cpp` (Catch2), test protocol `tests/proto/test_stream.proto`, test server in
  `tests/support/`.
- Benchmarks: `benchmarks/` (Catch2 benchmarking, hidden test cases selected by tag).
- Build logic: `CMakeLists.txt`, `cmake/*.cmake`, `CMakePresets.json`, `conanfile.py`.

## Build and test

Use the presets of [`CMakePresets.json`](CMakePresets.json); each builds in `build-<preset>/` with Ninja.

```bash
cmake --workflow --preset conan-release           # configure + build + unit/integration tests
cmake --workflow --preset conan-relwithsanitizers # same with ASan/UBSan (+LSan on Linux)
cmake --workflow --preset conan-coverage          # Debug tests with coverage (GCC/Clang), then:
gcovr build-conan-coverage                        # coverage summary (settings in gcovr.cfg)
```

Step by step, or to iterate after the first configure:

```bash
cmake --preset conan-release
cmake --build --preset conan-release
ctest --preset conan-release                      # excludes benchmarks
ctest --preset conan-release-benchmarks           # benchmarks only
```

- `conan-*` presets take dependencies from Conan 2 (needs `conan` on `PATH`); the first configure builds gRPC
  once and the Conan cache keeps it. `cpm-*` presets build gRPC from source with CPM (slow the first time;
  set `CPM_SOURCE_CACHE=$HOME/.cache/CPM` to share downloads).
- Machine-specific settings (e.g. `CONAN_COMMAND`, compiler paths) go into an untracked
  `CMakeUserPresets.json` inheriting from these presets, or on the command line — **never commit them to
  `CMakePresets.json`**.
- Run a single suite or test case directly:

  ```bash
  ./build-conan-release/tests/GrpcAsyncStreamTests "[write-queue]"
  ./build-conan-release/tests/GrpcAsyncStreamTests "Name of the test case"
  ./build-conan-release/tests/GrpcAsyncStreamTests --list-tests
  ./build-conan-release/benchmarks/GrpcAsyncStreamBenchmarks "[load]"
  ```

- Suite tags: `[lifecycle]`, `[write-queue]`, `[write-stream]`, `[read-stream]`, `[integration]`, `[mocks]`,
  `[stream-utils]`, `[test-utils]`. Benchmark tags: `[latency]`, `[throughput]`, `[read]`, `[write]`, `[queue]`,
  `[poll]`, `[load]` (all under `[benchmark]`).

### Definition of done

Before proposing a change, at minimum:

1. `cmake --workflow --preset conan-release` passes (warnings are errors: `-Wall -Wextra -Wpedantic -Wshadow
   -Wconversion -Werror`, `/W4 /WX` on MSVC).
2. For changes to library or test-utility code, `cmake --workflow --preset conan-relwithsanitizers` passes too.
3. **Test coverage does not go down.** New or changed code in `include/` and `tests/include/` is covered by
   tests, the total line coverage measured by `conan-coverage` + `gcovr` is not lower than on `main`, and
   branch coverage (`gcovr build-conan-coverage --txt-metric branch`) stays at 90% or more.
   CI uploads the report to [Codecov](https://codecov.io/gh/egorodet/GrpcAsyncStream), whose project and
   patch checks (see [`codecov.yml`](codecov.yml)) fail a pull request that lowers coverage.
4. `GrpcAsyncStreamHeaderChecks` builds (it is part of the build): every public header must compile on its
   own. A new public header must be added to the header list in [`tests/CMakeLists.txt`](tests/CMakeLists.txt)
   (test utility headers to `GRPC_ASYNC_STREAM_TEST_UTILS_HEADERS` in `CMakeLists.txt`).
5. Docs are updated: [`README.md`](README.md) for API, options, tests, layout or benchmark changes;
   [`docs/Optimizations.md`](docs/Optimizations.md) for performance work.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs Linux GCC/Clang, macOS Apple Clang and
Windows MSVC, with and without sanitizers, a Linux GCC coverage build and a CPM build. Code must stay portable across all of them:
include every standard header you use (MSVC does not provide transitive includes the way libstdc++ does,
e.g. `<functional>` for `std::ranges::greater_equal`).

## Code conventions

- **C++20**, header-only library; use concepts, designated initializers, defaulted `operator==` where it fits.
- **Naming**: types and enum values `PascalCase`; functions and methods `camelCase`; local variables, struct
  fields and parameters `snake_case`; private data members `m_snake_case`; constants `snake_case`.
- Namespaces: `grpc_async_stream` (library, `detail` for internals), `grpc_async_stream_test` (test
  utilities), `TestSupport` (this repo's test server). Close namespaces with `} // namespace name`.
- Braces on their own line for namespaces, classes, functions and control blocks; 4-space indentation;
  aligned `=` / comments in field blocks; lines up to ~120 characters. Match the surrounding code.
- Every source file starts with the MIT header:

  ```cpp
  // Copyright (c) 2026 Evgeny Gorodetskiy
  // SPDX-License-Identifier: MIT
  // Licensed under the MIT License. See the LICENSE file in the project root for details.
  ```

  followed by `#pragma once` for headers and a 1–2 line comment describing the file.
- Include order: project headers (`"support/..."`, then `<grpc_async_stream_test/...>`,
  `<grpc_async_stream/...>`), then third-party (`<grpcpp/...>`, `<catch2/...>`), then the standard library;
  each group sorted. Include only what you use.
- **Comments are short**: explain *why*, not *what*; no comment block longer than 5 lines, most 1–2 lines.
- `[[nodiscard]]` on queries and results that must not be ignored; `noexcept` where it holds.

## Library design rules

These keep gRPC async usage correct; do not break them:

- Single-threaded and poll-driven: `sendRequest()` and `poll()` never block; only `pollFor()` waits.
- At most one outstanding write; none before the start-call completes; one read outstanding for the call.
- `Finish` exactly once, after all other operations; the completion queue is drained before anything it
  references is destroyed. `close()` and the destructor cancel the call; streams can be reopened.
- Streams are composed by **single inheritance with a `Base` template parameter** and **no virtual
  functions** (`AsyncReadStream` → `AsyncWriteStream` → `detail::AsyncCallBase`). Each layer extends
  `open()`, `close()` and `handleEvent()` of the layer below.
- The write queue is bounded by messages *and* bytes. Keep the `WriteQueueStats` invariant
  (`submitted == written + dropped + discarded + queued + in_flight`) true after every operation.
- The library depends on gRPC and the standard library only; magic_enum and Catch2 belong to the test
  utilities, tests and benchmarks.

## Tests

- Every behaviour change or bug fix gets a test in the matching suite file.
- Keep coverage at least where it was: when adding code paths (error handling, new options, policies), add
  tests that reach them; check the uncovered lines with `gcovr build-conan-coverage --txt` or
  `--html-details`. Do not exclude code from coverage to hide a drop.
- Prefer the deterministic fakes (`FakeCall`, `makeFakeStreamFactory()` / `makeFakeWriterFactory()` /
  `makeFakeReaderFactory()`) for state-machine and ordering tests; use the in-process server
  (`TestSupport` + `InProcessServer`) for real gRPC behaviour. Check `call->violations().empty()` with fakes.
- New server behaviour for tests goes into `tests/proto/test_stream.proto` and `tests/support/TestServer.*`.
- Over the in-process transport, a write issued after the server returned its status stays pending until the
  client requests the status; write-only tests with an early server reply should half-close instead.
- Keep tests fast and free of fixed sleeps; use `pollUntil()` / `pollFor()` with a deadline.
- `tests/TestUtilsUsageTests.cpp` must use only the public headers, as an external product would.

## CMake and dependencies

- Options are prefixed `GRPC_ASYNC_STREAM_` and documented in the README's *CMake options* table.
- Dependencies are resolved in `cmake/Dependencies.cmake`: imported targets → installed package → CPM fetch.
  Dependency versions are set in two places: the `GRPC_ASYNC_STREAM_*_VERSION` option defaults (CPM fetch)
  and `conanfile.py` (Conan); review both when bumping one, and update the README.
- `GRPC_ASYNC_STREAM_BUILD_TEST_UTILS` is `OFF` by default so the library alone never needs magic_enum; this
  project's tests define the TestUtils target without installing it.
- Compiler flags and sanitizer settings live in `cmake/CompilerOptions.cmake` and apply only to this
  project's targets, never to gRPC.

## Git and pull requests

- Never commit to `main`; create a branch named `<type>/<short-kebab-description>` (e.g. `fix`,
  `refactor`, `cleanup`, `test`, `docs`) and open a pull request against `main`.
- Commit messages: imperative subject line ≤ ~72 characters without a trailing period, blank line, then a
  wrapped body explaining what changed and **why**.
- Do not commit build directories (`build*/`, `cmake-build-*/`), `CMakeUserPresets.json`, IDE files or
  machine-specific paths.
