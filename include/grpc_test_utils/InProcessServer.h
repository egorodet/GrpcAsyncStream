#pragma once

// A gRPC server running inside the test process, reachable through in-process
// channels: no network ports, no port conflicts between parallel test runs.
// Works with any synchronous, callback or async gRPC service implementation.
//
//   MyServiceImpl service;
//   grpc_test_utils::InProcessServer server(service);   // or server({&service_a, &service_b})
//   auto stub = server.makeStub<my::proto::MyService>();
//
// Test-framework independent; requires only gRPC.

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <functional>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string_view>

namespace grpc_test_utils
{

class InProcessServer
{
public:
    // Optional hook to adjust the server before it starts (resource quota,
    // completion queues of async services, options, ...)
    using Configure = std::function<void(grpc::ServerBuilder&)>;

    // Registers the service (which must outlive the server) and starts it
    explicit InProcessServer(grpc::Service& service, const Configure& configure = {},
                             std::chrono::milliseconds shutdown_grace_period = std::chrono::seconds(1))
        : InProcessServer({&service}, configure, shutdown_grace_period)
    {
    }

    // Registers the services (which must outlive the server) and starts it
    explicit InProcessServer(std::initializer_list<grpc::Service*> services, const Configure& configure = {},
                             std::chrono::milliseconds shutdown_grace_period = std::chrono::seconds(1))
        : m_shutdown_grace_period(shutdown_grace_period)
    {
        grpc::ServerBuilder builder;
        for (grpc::Service* service : services)
            builder.RegisterService(service);
        if (configure)
            configure(builder);

        m_server = builder.BuildAndStart();
        if (!m_server)
            throw std::runtime_error("InProcessServer: failed to start the gRPC server");

        grpc::ChannelArguments args;
        m_channel = m_server->InProcessChannel(args);
    }

    ~InProcessServer() { shutdown(); }

    InProcessServer(const InProcessServer&)            = delete;
    InProcessServer& operator=(const InProcessServer&) = delete;
    InProcessServer(InProcessServer&&)                 = delete;
    InProcessServer& operator=(InProcessServer&&)      = delete;

    // Channel connected to this server in-process
    [[nodiscard]] std::shared_ptr<grpc::Channel> channel() const { return m_channel; }

    // Additional in-process channel with its own arguments (e.g. message size limits)
    [[nodiscard]] std::shared_ptr<grpc::Channel> makeChannel(const grpc::ChannelArguments& args) const
    {
        return m_server->InProcessChannel(args);
    }

    // Fresh stub of the given generated service class over the default channel,
    // e.g. makeStub<my::proto::MyService>()
    template<typename GeneratedService>
    [[nodiscard]] std::unique_ptr<typename GeneratedService::Stub> makeStub() const
    {
        return GeneratedService::NewStub(m_channel);
    }

    [[nodiscard]] grpc::Server& server() { return *m_server; }

    // Stops accepting calls, gives active calls the grace period to end, then
    // cancels them and waits until all handlers have returned. Idempotent.
    void shutdown()
    {
        if (!m_server)
            return;
        m_server->Shutdown(std::chrono::system_clock::now() + m_shutdown_grace_period);
        m_server->Wait();
        m_server.reset();
    }

private:
    std::chrono::milliseconds      m_shutdown_grace_period;
    std::unique_ptr<grpc::Server>  m_server;
    std::shared_ptr<grpc::Channel> m_channel;
};

// A server address that refuses connections on every platform, for testing
// connection failures (UNAVAILABLE)
#if defined(_WIN32)
inline constexpr std::string_view unreachable_server_address = "ipv4:127.0.0.1:1";
#else
inline constexpr std::string_view unreachable_server_address = "unix:/nonexistent/grpc-async-stream.sock";
#endif

} // namespace grpc_test_utils
