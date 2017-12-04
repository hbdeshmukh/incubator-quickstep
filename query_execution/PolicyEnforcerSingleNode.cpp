/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include "query_execution/PolicyEnforcerSingleNode.hpp"

#include <cstddef>
#include <memory>
#include <queue>
#include <utility>
#include <unordered_map>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/QueryManagerBase.hpp"
#include "query_execution/QueryManagerSingleNode.hpp"
#include "query_execution/WorkerDirectory.hpp"
#include "query_execution/WorkerMessage.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "transaction/ConcurrencyControl.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

namespace quickstep {

DEFINE_uint64(max_msgs_per_dispatch_round, 20, "Maximum number of messages that"
              " can be allocated in a single round of dispatch of messages to"
              " the workers.");

void PolicyEnforcerSingleNode::getWorkerMessages(
    std::vector<std::unique_ptr<WorkerMessage>> *worker_messages) {
  // Iterate over admitted queries until either there are no more
  // messages available, or the maximum number of messages have
  // been collected.
  DCHECK(worker_messages->empty());
  // TODO(harshad) - Make this function generic enough so that it
  // works well when multiple queries are getting executed.
  std::size_t per_query_share = 0;
  if (!admitted_queries_.empty()) {
    per_query_share = FLAGS_max_msgs_per_dispatch_round / admitted_queries_.size();
  } else {
    LOG(WARNING) << "Requesting WorkerMessages when no query is running";
    return;
  }
  DCHECK_GT(per_query_share, 0u);
  std::vector<std::size_t> finished_queries_ids;

  for (const auto &admitted_query_info : admitted_queries_) {
    QueryManagerBase *curr_query_manager = admitted_query_info.second.get();
    DCHECK(curr_query_manager != nullptr);
    std::size_t messages_collected_curr_query = 0;
    while (messages_collected_curr_query < per_query_share) {
      WorkerMessage *next_worker_message =
          static_cast<QueryManagerSingleNode*>(curr_query_manager)->getNextWorkerMessage(0, kAnyNUMANodeID);
      if (next_worker_message != nullptr) {
        ++messages_collected_curr_query;
        worker_messages->push_back(std::unique_ptr<WorkerMessage>(next_worker_message));
      } else {
        // No more work ordes from the current query at this time.
        // Check if the query's execution is over.
        if (curr_query_manager->getQueryExecutionState().hasQueryExecutionFinished()) {
          // If the query has been executed, remove it.
          finished_queries_ids.push_back(admitted_query_info.first);
        }
        break;
      }
    }
  }
  for (const std::size_t finished_qid : finished_queries_ids) {
    removeQuery(finished_qid);
  }
}

bool PolicyEnforcerSingleNode::admitQuery(QueryHandle *query_handle) {
  const std::size_t query_id = query_handle->query_id();
  // Check if the query with the same ID is present.
  DCHECK(admitted_queries_.end() != admitted_queries_.find(query_id));
  admitted_queries_[query_id].reset(
      new QueryManagerSingleNode(foreman_client_id_, num_numa_nodes_, query_handle,
                                 catalog_database_, storage_manager_, bus_));
  return true;
}

void PolicyEnforcerSingleNode::removeQuery(const std::size_t query_id) {
  concurrency_control_->signalTransactionCompletion(query_id);
  admitted_queries_.erase(query_id);
}

bool PolicyEnforcerSingleNode::hasQueries() const {
  return concurrency_control_->getWaitingTransactionsCount() > 0u;
}

bool PolicyEnforcerSingleNode::admitQueries(const std::vector<QueryHandle *> &query_handles) {
  for (auto handle : query_handles) {
    admitQuery(handle);
  }
  return true;
}

void PolicyEnforcerSingleNode::checkQueryCompletion(size_t query_id, QueryManagerBase::dag_node_index op_index) {
  if (admitted_queries_[query_id]->queryStatus(op_index) ==
      QueryManagerBase::QueryStatusCode::kQueryExecuted) {
    onQueryCompletion(admitted_queries_[query_id].get());

    removeQuery(query_id);
    if (hasQueries()) {
      // Admit the earliest waiting query.
      QueryHandle *new_query = concurrency_control_->getNextTransactionForExecution();
      admitQuery(new_query);
    }
  }

  PolicyEnforcerBase::checkQueryCompletion(query_id, op_index);
}

}  // namespace quickstep
