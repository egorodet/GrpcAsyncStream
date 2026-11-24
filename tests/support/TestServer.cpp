#include "support/TestServer.h"

#include <algorithm>
#include <optional>
#include <thread>

namespace TestSupport
{

namespace
{

// Waits for the given time while staying responsive to call cancellation, so
// slow-server tests do not delay the server shutdown. The timer granularity of
// sleep_for is about 1 ms on Windows (and coarse on macOS), which would make
// microsecond delays 10-50x longer than requested there; the last stretch of a
// wait is therefore spent yielding instead of sleeping, on every platform alike.
void interruptibleSleep(grpc::ServerContext* context, std::chrono::microseconds delay)
{
    using clock                       = std::chrono::steady_clock;
    constexpr auto sleep_granularity  = std::chrono::milliseconds(2);
    constexpr auto cancellation_check = std::chrono::milliseconds(5);

    const auto deadline = clock::now() + delay;
    while (!context->IsCancelled())
    {
        const auto remaining = deadline - clock::now();
        if (remaining <= clock::duration::zero())
            return;
        if (remaining > sleep_granularity)
            std::this_thread::sleep_for(std::min<clock::duration>(remaining - sleep_granularity, cancellation_check));
        else
            std::this_thread::yield();
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

std::string clientMetadataValue(const grpc::ServerContext* context)
{
    const auto& client_metadata = context->client_metadata();
    if (auto it = client_metadata.find(grpc::string_ref(metadata_key.data(), metadata_key.size()));
        it != client_metadata.end())
        return std::string(it->second.data(), it->second.size());
    return {};
}

} // namespace

grpc::Status TestStreamService::Chat(grpc::ServerContext*                         context,
                                     grpc::ServerReaderWriter<Response, Request>* stream)
{
    ActiveCallGuard active(m_active_calls);

    const std::string metadata_value = clientMetadataValue(context);

    std::uint64_t server_seq = 0;
    auto          respond    = [&](std::uint64_t id, const std::string& payload) -> bool {
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

    // Handles one request: the status ends the call, std::nullopt keeps it going,
    // and `false` in `stream_ok` means a response could not be written.
    bool stream_ok    = true;
    auto handle       = [&](const Request& request) -> std::optional<grpc::Status> {
        switch (request.command())
        {
        case proto::TestRequest::ECHO:
            stream_ok = respond(request.id(), request.payload());
            return std::nullopt;

        case proto::TestRequest::BURST:
            for (std::uint32_t i = 0; i < request.count() && stream_ok; ++i)
                stream_ok = respond(request.id() + i, request.payload());
            return std::nullopt;

        case proto::TestRequest::FAIL:
            return grpc::Status(static_cast<grpc::StatusCode>(request.status_code()), request.payload());

        case proto::TestRequest::END:
            return grpc::Status::OK;

        default:
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unknown command");
        }
    };

    Request request;
    while (stream_ok)
    {
        if (const auto delay = m_read_delay_us.load(); delay > 0)
            interruptibleSleep(context, std::chrono::microseconds(delay));

        if (!stream->Read(&request))
            break; // client half-closed or cancelled

        ++m_received_requests;

        if (request.delay_us() > 0)
            interruptibleSleep(context, std::chrono::microseconds(request.delay_us()));

        if (std::optional<grpc::Status> final_status = handle(request))
        {
            ++m_finished_calls;
            return *std::move(final_status);
        }
    }

    if (context->IsCancelled())
    {
        ++m_cancelled_calls;
        return grpc::Status::CANCELLED;
    }
    ++m_finished_calls;
    return grpc::Status::OK;
}

grpc::Status TestStreamService::Upload(grpc::ServerContext* context, grpc::ServerReader<Request>* reader,
                                       UploadSummary* summary)
{
    ActiveCallGuard active(m_active_calls);

    summary->set_metadata_value(clientMetadataValue(context));

    Request request;
    while (true)
    {
        if (const auto delay = m_read_delay_us.load(); delay > 0)
            interruptibleSleep(context, std::chrono::microseconds(delay));

        if (!reader->Read(&request))
            break; // client half-closed or cancelled

        ++m_received_requests;

        if (request.delay_us() > 0)
            interruptibleSleep(context, std::chrono::microseconds(request.delay_us()));

        if (request.command() == proto::TestRequest::FAIL)
        {
            ++m_finished_calls;
            return grpc::Status(static_cast<grpc::StatusCode>(request.status_code()), request.payload());
        }

        summary->set_received_count(summary->received_count() + 1);
        summary->add_ids(request.id());
        summary->set_payload_bytes(summary->payload_bytes() + request.payload().size());

        if (request.command() == proto::TestRequest::END)
        {
            ++m_finished_calls;
            return grpc::Status::OK;
        }
    }

    if (context->IsCancelled())
    {
        ++m_cancelled_calls;
        return grpc::Status::CANCELLED;
    }
    ++m_finished_calls;
    return grpc::Status::OK;
}

grpc::Status TestStreamService::Subscribe(grpc::ServerContext* context, const Request* request,
                                          grpc::ServerWriter<Response>* writer)
{
    ActiveCallGuard active(m_active_calls);
    ++m_received_requests;

    const std::string metadata_value = clientMetadataValue(context);
    if (request->delay_us() > 0)
        interruptibleSleep(context, std::chrono::microseconds(request->delay_us()));

    std::uint64_t server_seq   = 0;
    bool          write_failed = false;
    auto          respond      = [&](std::uint64_t id) -> bool {
        Response response;
        response.set_id(id);
        response.set_payload(request->payload());
        response.set_metadata_value(metadata_value);
        response.set_server_seq(server_seq++);
        if (!writer->Write(response))
        {
            write_failed = true;
            return false;
        }
        ++m_sent_responses;
        return true;
    };

    switch (request->command())
    {
    case proto::TestRequest::ECHO:
        respond(request->id());
        break;

    case proto::TestRequest::BURST:
        for (std::uint32_t i = 0; i < request->count(); ++i)
            if (!respond(request->id() + i))
                break;
        break;

    case proto::TestRequest::FAIL:
        ++m_finished_calls;
        return grpc::Status(static_cast<grpc::StatusCode>(request->status_code()), request->payload());

    case proto::TestRequest::END:
        break;

    default:
        ++m_finished_calls;
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unknown command");
    }

    // A server-streaming write fails only when the call is gone: the client
    // cancelled it (IsCancelled() may not report that yet at this point)
    if (write_failed || context->IsCancelled())
    {
        ++m_cancelled_calls;
        return grpc::Status::CANCELLED;
    }
    ++m_finished_calls;
    return grpc::Status::OK;
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
