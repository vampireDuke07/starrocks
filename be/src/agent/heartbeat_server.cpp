// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/heartbeat_server.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "agent/heartbeat_server.h"

#include <thrift/TProcessor.h>

#include <ctime>
#include <fstream>

#include "common/status.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/Status_types.h"
#include "runtime/heartbeat_flags.h"
#include "service/backend_options.h"
#include "storage/storage_engine.h"
#include "storage/utils.h"
#include "util/debug_util.h"
#include "util/network_util.h"
#include "util/thrift_server.h"

using std::fstream;
using std::nothrow;
using std::string;
using std::vector;
using apache::thrift::transport::TProcessor;

namespace starrocks {

HeartbeatServer::HeartbeatServer(TMasterInfo* master_info) : _master_info(master_info), _epoch(0) {
    _olap_engine = StorageEngine::instance();
}

void HeartbeatServer::init_cluster_id() {
    _master_info->cluster_id = _olap_engine->effective_cluster_id();
}

void HeartbeatServer::heartbeat(THeartbeatResult& heartbeat_result, const TMasterInfo& master_info) {
    //print heartbeat in every minute
    LOG_EVERY_N(INFO, 12) << "get heartbeat from FE."
                          << "host:" << master_info.network_address.hostname
                          << ", port:" << master_info.network_address.port << ", cluster id:" << master_info.cluster_id
                          << ", counter:" << google::COUNTER;

    // do heartbeat
    Status st = _heartbeat(master_info);
    st.to_thrift(&heartbeat_result.status);

    static int32_t num_hardware_cores = std::thread::hardware_concurrency();
    if (st.ok()) {
        heartbeat_result.backend_info.__set_be_port(config::be_port);
        heartbeat_result.backend_info.__set_http_port(config::webserver_port);
        heartbeat_result.backend_info.__set_be_rpc_port(-1);
        heartbeat_result.backend_info.__set_brpc_port(config::brpc_port);
#ifdef USE_STAROS
        heartbeat_result.backend_info.__set_starlet_port(config::starlet_port);
#endif
        heartbeat_result.backend_info.__set_version(get_short_version());
        heartbeat_result.backend_info.__set_num_hardware_cores(num_hardware_cores);
    }
}

Status HeartbeatServer::_heartbeat(const TMasterInfo& master_info) {
    std::lock_guard<std::mutex> lk(_hb_mtx);

    if (master_info.__isset.backend_ip) {
        if (master_info.backend_ip != BackendOptions::get_localhost()) {
            LOG(INFO) << master_info.backend_ip << " not equal to to backend localhost "
                      << BackendOptions::get_localhost();
            if (is_valid_ip(master_info.backend_ip)) {
                LOG(WARNING) << "backend ip saved in master does not equal to backend local ip"
                             << master_info.backend_ip << " vs. " << BackendOptions::get_localhost();
                std::stringstream ss;
                ss << "actual backend local ip: " << BackendOptions::get_localhost();
                return Status::InternalError(ss.str());
            }

            std::string ip = hostname_to_ip(master_info.backend_ip);
            if (ip.empty()) {
                std::stringstream ss;
                ss << "can not get ip from fqdn: " << master_info.backend_ip;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }

            std::vector<InetAddress> hosts;
            Status status = get_hosts_v4(&hosts);
            if (!status.ok() || hosts.empty()) {
                std::stringstream ss;
                ss << "the status was not ok when get_hosts_v4, error is " << status.get_error_msg();
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }

            bool set_new_localhost = false;

            for (std::vector<InetAddress>::iterator addr_it = hosts.begin(); addr_it != hosts.end(); ++addr_it) {
                if (addr_it->is_address_v4() && addr_it->get_host_address_v4() == ip) {
                    BackendOptions::set_localhost(master_info.backend_ip);
                    set_new_localhost = true;
                    break;
                }
            }

            if (!set_new_localhost) {
                std::stringstream ss;
                ss << "the host recorded in master is " << master_info.backend_ip
                   << ", but we cannot found the local ip that mapped to that host." << BackendOptions::get_localhost();
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }

            LOG(INFO) << "update localhost done, the new localhost is " << BackendOptions::get_localhost();
        }
    }

    // Check cluster id
    if (_master_info->cluster_id == -1) {
        LOG(INFO) << "get first heartbeat. update cluster id";
        // write and update cluster id
        auto st = _olap_engine->set_cluster_id(master_info.cluster_id);
        if (!st.ok()) {
            LOG(WARNING) << "fail to set cluster id. status=" << st.get_error_msg();
            return Status::InternalError("fail to set cluster id.");
        } else {
            std::string LOCALHOST = "127.0.0.1";
            if ((master_info.network_address.hostname == LOCALHOST) && (master_info.backend_ip != LOCALHOST)) {
                std::stringstream ss;
                ss << "FE heartbeat with localhost ip but BE is not deployed on the same machine "
                   << master_info.backend_ip;
                return Status::InternalError(ss.str());
            } else {
                _master_info->cluster_id = master_info.cluster_id;
                LOG(INFO) << "record cluster id. host: " << master_info.network_address.hostname
                          << ". port: " << master_info.network_address.port
                          << ". cluster id: " << master_info.cluster_id;
            }
        }
    } else {
        if (_master_info->cluster_id != master_info.cluster_id) {
            LOG(WARNING) << "ignore invalid cluster id: " << master_info.cluster_id;
            return Status::InternalError("invalid cluster id. ignore.");
        }
    }

    bool need_report = false;
    if (_master_info->network_address.hostname != master_info.network_address.hostname ||
        _master_info->network_address.port != master_info.network_address.port) {
        if (master_info.epoch > _epoch) {
            _master_info->network_address.hostname = master_info.network_address.hostname;
            _master_info->network_address.port = master_info.network_address.port;
            _epoch = master_info.epoch;
            need_report = true;
            LOG(INFO) << "master change. new master host: " << _master_info->network_address.hostname
                      << ". port: " << _master_info->network_address.port << ". epoch: " << _epoch;
        } else {
            LOG(WARNING) << "epoch is not greater than local. ignore heartbeat. host: "
                         << _master_info->network_address.hostname << " port: " << _master_info->network_address.port
                         << " local epoch: " << _epoch << " received epoch: " << master_info.epoch;
            return Status::InternalError("epoch is not greater than local. ignore heartbeat.");
        }
    } else {
        // when Master FE restarted, host and port remains the same, but epoch will be increased.
        if (master_info.epoch > _epoch) {
            _epoch = master_info.epoch;
            need_report = true;
            LOG(INFO) << "master restarted. epoch: " << _epoch;
        }
    }

    if (master_info.__isset.token) {
        if (!_master_info->__isset.token) {
            _master_info->__set_token(master_info.token);
            LOG(INFO) << "get token.  token: " << _master_info->token;
        } else if (_master_info->token != master_info.token) {
            LOG(WARNING) << "invalid token. local_token:" << _master_info->token << ". token:" << master_info.token;
            return Status::InternalError("invalid token.");
        }
    }

    if (master_info.__isset.http_port) {
        _master_info->__set_http_port(master_info.http_port);
    }

    if (master_info.__isset.heartbeat_flags) {
        HeartbeatFlags* heartbeat_flags = ExecEnv::GetInstance()->heartbeat_flags();
        heartbeat_flags->update(master_info.heartbeat_flags);
    }

    if (master_info.__isset.backend_id) {
        _master_info->__set_backend_id(master_info.backend_id);
    }

    if (need_report) {
        LOG(INFO) << "Master FE is changed or restarted. report tablet and disk info immediately";
        _olap_engine->trigger_report();
    }

    return Status::OK();
}

AgentStatus create_heartbeat_server(ExecEnv* exec_env, uint32_t server_port, ThriftServer** thrift_server,
                                    uint32_t worker_thread_num, TMasterInfo* local_master_info) {
    HeartbeatServer* heartbeat_server = new (nothrow) HeartbeatServer(local_master_info);
    if (heartbeat_server == nullptr) {
        return STARROCKS_ERROR;
    }

    heartbeat_server->init_cluster_id();

    std::shared_ptr<HeartbeatServer> handler(heartbeat_server);
    std::shared_ptr<TProcessor> server_processor(new HeartbeatServiceProcessor(handler));
    string server_name("heartbeat");
    *thrift_server =
            new ThriftServer(server_name, server_processor, server_port, exec_env->metrics(), worker_thread_num);
    return STARROCKS_SUCCESS;
}
} // namespace starrocks
