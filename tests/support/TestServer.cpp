#include "support/TestServer.h"

#include <algorithm>
#include <thread>

namespace TestSupport
{

namespace
{

// Sleeps for the given time while staying responsive to call cancellation,
// so slow-server tests do not delay the server shutdown.
void interruptibleSleep(grpc::ServerContext* context, std::chrono::microseconds delay)
{
    using clock         = std::chrono::steady_clock;
    const auto deadline = clock::now() + delay;
    while (!context->IsCancelled())
    {
        const auto now = clock::now();
        if (now >= deadline)
            return;
        std::this_thread::sleep_for(std::min<clock::duration>(deadline - now, std::chrono::milliseconds(5)));
    }
}

class ActiveCallGuard
{
public:
    explicit ActiveCallGuard(std::atomic<std::uint64_t>& counter)
        : m_counter(counter)
    {
        ++m_counter;
    }
    ~ActiveCallGuard() { --m_counter; }

    ActiveCallGuard(const ActiveCallGuard&)            = delete;
    ActiveCallGuard& operator=(const ActiveCallGuard&) = delete;

private:
    std::atomic<std::uint64_t>& m_counter;
};

} // namespace

grpc::Status TestStreamService::Chat(grpc::ServerContext*                         context,
                                     grpc::ServerReaderWriter<Response, Request>* stream)
{
    ActiveCallGuard active(m_active_calls);

    std::string metadata_value;
    const auto& client_metadata = context->client_metadata();
    if (auto it = client_metadata.find(metadata_key); it != client_metadata.end())
        metadata_value.assign(it->second.data(), it->second.size());

    std::uint64_t server_seq = 0;
    auto          respond    = [&](std::uint64_t id, const std::string& payload) {
        Response response;
        response.set_id(id);
        response.set_payload(payload);
        response.set_metadata_value(metadata_value);
        response.set_server_seq(server_seq++);
        if (!stream->Write(response))
            return false;
        ++m_sent_responses;
        return true;
    };

    Request request;
    for (;;)
    {
        if (const auto delay = m_read_delay_us.load(); delay > 0)
            interruptibleSleep(context, std::chrono::microseconds(delay));

        if (!stream->Read(&request))
            break; // client half-closed or cancelled

        ++m_received_requests;

        if (request.delay_us() > 0)
            interruptibleSleep(context, std::chrono::microseconds(request.delay_us()));

        switch (request.command())
        {
        case proto::TestRequest::ECHO:
            if (!respond(request.id(), request.payload()))
                goto stream_broken;
            break;

        case proto::TestRequest::BURST:
            for (std::uint32_t i = 0; i < request.count(); ++i)
                if (!respond(request.id() + i, request.payload()))
                    goto stream_broken;
            break;

        case proto::TestRequest::FAIL:
            ++m_finished_calls;
            return grpc::Status(static_cast<grpc::StatusCode>(request.status_code()), request.payload());

        case proto::TestRequest::END:
            ++m_finished_calls;
            return grpc::Status::OK;

        default:
            ++m_finished_calls;
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unknown command");
        }
    }

stream_broken:
    if (context->IsCancelled())
    {
        ++m_cancelled_calls;
        return grpc::Status::CANCELLED;
    }
    ++m_finished_calls;
    return grpc::Status::OK;
}

TestServer::TestServer()
{
    grpc::ServerBuilder builder;
    builder.RegisterService(&m_service);
    m_server = builder.BuildAndStart();

    grpc::ChannelArguments args;
    m_channel = m_server->InProcessChannel(args);
}

TestServer::~TestServer()
{
    // Give active calls a moment to end gracefully, then cancel them
    m_server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
    m_server->Wait();
}

std::unique_ptr<proto::TestStream::Stub> TestServer::makeStub() const
{
    return proto::TestStream::NewStub(m_channel);
}

Request makeEcho(std::uint64_t id, std::string payload)
{
    Request request;
    request.set_id(id);
    request.set_command(proto::TestRequest::ECHO);
    request.set_payload(std::move(payload));
    return request;
}

Request makeBurst(std::uint64_t id, std::uint32_t count, std::string payload)
{
    Request request;
    request.set_id(id);
    request.set_command(proto::TestRequest::BURST);
    request.set_count(count);
    request.set_payload(std::move(payload));
    return request;
}

Request makeFail(grpc::StatusCode code, std::string message)
{
    Request request;
    request.set_command(proto::TestRequest::FAIL);
    request.set_status_code(static_cast<std::int32_t>(code));
    request.set_payload(std::move(message));
    return request;
}

Request makeEnd()
{
    Request request;
    request.set_command(proto::TestRequest::END);
    return request;
}

} // namespace TestSupport
