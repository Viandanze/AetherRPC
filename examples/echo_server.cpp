// AetherRPC example: echo server.
//
//   ./echo_server [port]

#include <memory>
#include <string>

#include "base/Logger.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "rpc/RpcServer.h"
#include "echo.pb.h"

using namespace aether::rpc;

// --- business service implementations ------------------------------------

class EchoServiceImpl final : public ::aether::rpc::example::EchoService {
public:
    void Echo(google::protobuf::RpcController* controller,
              const ::aether::rpc::example::EchoRequest* request,
              ::aether::rpc::example::EchoResponse* response,
              google::protobuf::Closure* done) override {
        (void)controller;
        std::string out;
        int repeat = request->repeat() > 0 ? request->repeat() : 1;
        for (int i = 0; i < repeat; ++i) out += request->message();
        response->set_message(std::move(out));
        if (done != nullptr) done->Run();
    }
};

class MathServiceImpl final : public ::aether::rpc::example::MathService {
public:
    void Add(google::protobuf::RpcController* controller,
             const ::aether::rpc::example::AddRequest* request,
             ::aether::rpc::example::AddResponse* response,
             google::protobuf::Closure* done) override {
        (void)controller;
        response->set_sum(request->a() + request->b());
        if (done != nullptr) done->Run();
    }
};

int main(int argc, char* argv[]) {
    uint16_t port = 8080;
    if (argc > 1) port = static_cast<uint16_t>(atoi(argv[1]));

    EventLoop loop;
    InetAddress listenAddr(port);
    RpcServer server(&loop, listenAddr, "echo-server");

    EchoServiceImpl echoService;
    MathServiceImpl mathService;
    server.registerService(&echoService);
    server.registerService(&mathService);

    server.setThreadNum(4);
    server.start();

    LOG_INFO("echo_server listening on %s", listenAddr.toIpPort().c_str());
    loop.loop();
    return 0;
}
