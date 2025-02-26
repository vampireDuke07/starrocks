// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <aws/core/Aws.h>
#include <gperftools/malloc_extension.h>
#include <sys/file.h>
#include <unistd.h>

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include "backend_service.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/pipeline/query_context.h"
#include "http_service.h"
#include "internal_service.h"
#include "runtime/exec_env.h"
#include "service/brpc_service.h"
#include "service/service.h"
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/thrift_server.h"

void start_be() {
    using starrocks::Status;

    auto* exec_env = starrocks::ExecEnv::GetInstance();

    // Begin to start services
    // 1. Start thrift server with 'be_port'.
    starrocks::ThriftServer* be_server = nullptr;
    EXIT_IF_ERROR(starrocks::BackendService::create_service(exec_env, starrocks::config::be_port, &be_server));
    Status status = be_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "StarRocks Be server did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    // 2. Start brpc service.
    std::unique_ptr<starrocks::BRpcService> brpc_service = std::make_unique<starrocks::BRpcService>(exec_env);
    status = brpc_service->start(starrocks::config::brpc_port,
                                 new starrocks::BackendInternalServiceImpl<starrocks::PInternalService>(exec_env),
                                 new starrocks::BackendInternalServiceImpl<doris::PBackendService>(exec_env));
    if (!status.ok()) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    // 3. Start http service.
    std::unique_ptr<starrocks::HttpServiceBE> http_service = std::make_unique<starrocks::HttpServiceBE>(
            exec_env, starrocks::config::webserver_port, starrocks::config::webserver_num_workers);
    status = http_service->start();
    if (!status.ok()) {
        LOG(ERROR) << "Internal Error:" << status.message();
        LOG(ERROR) << "StarRocks Be http service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    while (!starrocks::k_starrocks_exit) {
        sleep(10);
    }

    http_service.reset();
    brpc_service->join();
    brpc_service.reset();

    be_server->stop();
    be_server->join();
    delete be_server;
}
