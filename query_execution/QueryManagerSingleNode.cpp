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

#include "query_execution/QueryManagerSingleNode.hpp"

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/WorkerMessage.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "relational_operators/RebuildWorkOrder.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/StorageBlock.hpp"
#include "utility/DAG.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

class WorkOrder;

QueryManagerSingleNode::QueryManagerSingleNode(
    const tmb::client_id foreman_client_id,
    const std::size_t num_numa_nodes,
    QueryHandle *query_handle,
    CatalogDatabaseLite *catalog_database,
    StorageManager *storage_manager,
    tmb::MessageBus *bus)
    : QueryManagerBase(query_handle),
      foreman_client_id_(foreman_client_id),
      storage_manager_(DCHECK_NOTNULL(storage_manager)),
      bus_(DCHECK_NOTNULL(bus)),
      query_context_(new QueryContext(query_handle->getQueryContextProto(),
                                      *catalog_database,
                                      storage_manager_,
                                      foreman_client_id_,
                                      bus_)),
      workorders_container_(
          new WorkOrdersContainer(num_operators_in_dag_, num_numa_nodes)),
      next_working_operator_id_index_(0),
      next_waiting_operator_id_index_(0) {
  /*for (dag_node_index i = 0; i < num_operators_in_dag_; ++i) {
    working_set_operators_.emplace_back(i);
  }*/
  working_set_operators_.emplace_back(1);
  working_set_operators_.emplace_back(0);
  working_set_operators_.emplace_back(16);
  for (dag_node_index i = 0; i < num_operators_in_dag_; ++i) {
    if (std::find(std::begin(working_set_operators_),
                  std::end(working_set_operators_),
                  i) == std::end(working_set_operators_)) {
      waiting_set_operators_.emplace_back(i);
    }
  }
  // Collect all the workorders from all the relational operators in the DAG.
  for (dag_node_index index = 0; index < num_operators_in_dag_; ++index) {
    if (checkAllBlockingDependenciesMet(index)) {
      query_dag_->getNodePayloadMutable(index)->informAllBlockingDependenciesMet();
      processOperator(index, false);
    }
  }
}

std::pair<WorkerMessage *, std::size_t>
QueryManagerSingleNode::getNextWorkerMessageFromSet(
    const std::size_t start_index,
    const std::vector<dag_node_index> &operators_set,
    const numa_node_id numa_node) const {
  // Default policy: Operator with lowest index first.
  WorkOrder *work_order = nullptr;
  size_t num_operators_checked = 0;
  for (dag_node_index index = start_index;
       num_operators_checked < operators_set.size();
       index = (index + 1) % operators_set.size(), ++num_operators_checked) {
    const dag_node_index operator_id = operators_set[index];
    if (query_exec_state_->hasExecutionFinished(operator_id)) {
      continue;
    }
    if (numa_node != kAnyNUMANodeID) {
      // First try to get a normal WorkOrder from the specified NUMA node.
      work_order = workorders_container_->getNormalWorkOrderForNUMANode(operator_id, numa_node);
      if (work_order != nullptr) {
        // A WorkOrder found on the given NUMA node.
        query_exec_state_->incrementNumQueuedWorkOrders(operator_id);
        return std::make_pair(WorkerMessage::WorkOrderMessage(work_order, operator_id), index);
      } else {
        // Normal workorder not found on this node. Look for a rebuild workorder
        // on this NUMA node.
        work_order = workorders_container_->getRebuildWorkOrderForNUMANode(operator_id, numa_node);
        if (work_order != nullptr) {
          return std::make_pair(WorkerMessage::RebuildWorkOrderMessage(work_order, operator_id), index);
        }
      }
    }
    // Either no workorder found on the given NUMA node, or numa_node is
    // 'kAnyNUMANodeID'.
    // Try to get a normal WorkOrder from other NUMA nodes.
    work_order = workorders_container_->getNormalWorkOrder(operator_id);
    if (work_order != nullptr) {
      query_exec_state_->incrementNumQueuedWorkOrders(operator_id);
      return std::make_pair(WorkerMessage::WorkOrderMessage(work_order, operator_id), index);
    } else {
      // Normal WorkOrder not found, look for a RebuildWorkOrder.
      work_order = workorders_container_->getRebuildWorkOrder(operator_id);
      if (work_order != nullptr) {
        return std::make_pair(WorkerMessage::RebuildWorkOrderMessage(work_order, operator_id), index);
      }
    }
  }
  // No WorkOrders available right now.
  return std::make_pair(nullptr, 0);
}

WorkerMessage* QueryManagerSingleNode::getNextWorkerMessage(
    const dag_node_index start_operator_index, const numa_node_id numa_node) {
  // Default policy: Operator with lowest index first.
  auto return_pair = getNextWorkerMessageFromSet(
      next_working_operator_id_index_, working_set_operators_, numa_node);
  if (return_pair.first != nullptr) {
    next_working_operator_id_index_ =
        (return_pair.second + 1) % (working_set_operators_.size());
    return return_pair.first;
  } else {
    // Check for available work in the waiting operators' set.
    return_pair = getNextWorkerMessageFromSet(
      next_waiting_operator_id_index_, waiting_set_operators_, numa_node);
    if (return_pair.first != nullptr) {
      next_waiting_operator_id_index_ =
          (return_pair.second + 1) % (waiting_set_operators_.size());
      return return_pair.first;
    }
  }
  return nullptr;
}

bool QueryManagerSingleNode::fetchNormalWorkOrders(const dag_node_index index) {
  bool generated_new_workorders = false;
  if (!query_exec_state_->hasDoneGenerationWorkOrders(index)) {
    // Do not fetch any work units until all blocking dependencies are met.
    // The releational operator is not aware of blocking dependencies for
    // uncorrelated scalar queries.
    if (!checkAllBlockingDependenciesMet(index)) {
      return false;
    }
    const size_t num_pending_workorders_before =
        workorders_container_->getNumNormalWorkOrders(index);
    const bool done_generation =
        query_dag_->getNodePayloadMutable(index)->getAllWorkOrders(workorders_container_.get(),
                                                                   query_context_.get(),
                                                                   storage_manager_,
                                                                   foreman_client_id_,
                                                                   bus_);
    if (done_generation) {
      query_exec_state_->setDoneGenerationWorkOrders(index);
    }

    // TODO(shoban): It would be a good check to see if operator is making
    // useful progress, i.e., the operator either generates work orders to
    // execute or still has pending work orders executing. However, this will not
    // work if Foreman polls operators without feeding data. This check can be
    // enabled, if Foreman is refactored to call getAllWorkOrders() only when
    // pending work orders are completed or new input blocks feed.

    generated_new_workorders =
        (num_pending_workorders_before <
         workorders_container_->getNumNormalWorkOrders(index));
  }
  return generated_new_workorders;
}

bool QueryManagerSingleNode::initiateRebuild(const dag_node_index index) {
  DCHECK(!workorders_container_->hasRebuildWorkOrder(index));
  DCHECK(checkRebuildRequired(index));
  DCHECK(!checkRebuildInitiated(index));

  getRebuildWorkOrders(index, workorders_container_.get());

  query_exec_state_->setRebuildStatus(
      index, workorders_container_->getNumRebuildWorkOrders(index), true);

  return (query_exec_state_->getNumRebuildWorkOrders(index) == 0);
}

void QueryManagerSingleNode::getRebuildWorkOrders(const dag_node_index index,
                                                  WorkOrdersContainer *container) {
  const RelationalOperator &op = query_dag_->getNodePayload(index);
  const QueryContext::insert_destination_id insert_destination_index = op.getInsertDestinationID();

  if (insert_destination_index == QueryContext::kInvalidInsertDestinationId) {
    return;
  }

  std::vector<MutableBlockReference> partially_filled_block_refs;

  DCHECK(query_context_ != nullptr);
  InsertDestination *insert_destination = query_context_->getInsertDestination(insert_destination_index);
  DCHECK(insert_destination != nullptr);

  insert_destination->getPartiallyFilledBlocks(&partially_filled_block_refs);

  for (std::vector<MutableBlockReference>::size_type i = 0;
       i < partially_filled_block_refs.size();
       ++i) {
    container->addRebuildWorkOrder(
        new RebuildWorkOrder(query_id_,
                             std::move(partially_filled_block_refs[i]),
                             index,
                             op.getOutputRelationID(),
                             foreman_client_id_,
                             bus_),
        index);
  }
}

}  // namespace quickstep
