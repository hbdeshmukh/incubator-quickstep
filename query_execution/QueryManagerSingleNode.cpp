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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/LargestRemainingWorkFirstStrategy.hpp"
#include "query_execution/QueryExecutionTypedefs.hpp"
#include "query_execution/ShortestRemainingWorkFirstStrategy.hpp"
#include "query_execution/TopologicalSortStaticOrderStrategy.hpp"
#include "query_execution/WorkerMessage.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "relational_operators/RebuildWorkOrder.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/StorageBlock.hpp"
#include "utility/DAG.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace quickstep {

class WorkOrder;

DEFINE_int32(scheduling_strategy,
              0,
              "The scheduling strategy to be used. One of \"shortest remaining "
              "work first\", \"largest remaining work first\", \"static "
              "ordering topological sort\".");

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
      database_(static_cast<const CatalogDatabase&>(*catalog_database)) {
  switch (FLAGS_scheduling_strategy) {
    case kShortestRemainingWorkFirst: {
      scheduling_strategy_.reset(new ShortestRemainingWorkFirstStrategy(
          query_dag_, workorders_container_.get()));
      break;
    }
    case kLargestRemainingWorkFirst: {
      scheduling_strategy_.reset(new LargestRemainingWorkFirstStrategy());
      break;
    }
    case kStaticOrderTopoSort: {
      scheduling_strategy_.reset(new TopologicalSortStaticOrderStrategy());
      break;
    }
    default:
      std::cout << "Invalid strategy choice\n";
  }
  // Collect all the workorders from all the relational operators in the DAG.
  for (dag_node_index index = 0; index < num_operators_in_dag_; ++index) {
    if (checkAllBlockingDependenciesMet(index)) {
      query_dag_->getNodePayloadMutable(index)->informAllBlockingDependenciesMet();
      processOperator(index, false);
    }
  }
}

std::pair<QueryManagerBase::dag_node_index, int>
QueryManagerSingleNode::getHighestWaitingOperator() {
  if (!waiting_operators_.empty()) {
    auto it = std::max_element(
        waiting_operators_.begin(),
        waiting_operators_.end(),
        [this](const dag_node_index a, const dag_node_index b) {
          return compareOperatorsUsingRemainingWork(a, b);
        });
    DCHECK(it != waiting_operators_.end());
    return std::make_pair(*it,
                          workorders_container_->getNumTotalWorkOrders(*it));
  }
  return std::make_pair(0, -1);
}

std::pair<QueryManagerBase::dag_node_index, int>
QueryManagerSingleNode::getLowestWaitingOperatorNonZeroWork() {
  std::vector<dag_node_index> operators_with_work;
  int min_work = INT_MAX;
  std::size_t min_work_op_index = 0;
  for (auto op_id : waiting_operators_) {
    const int curr_op_pending_work =
        workorders_container_->getNumTotalWorkOrders(op_id);
    if (curr_op_pending_work < min_work && curr_op_pending_work > 0) {
      min_work = curr_op_pending_work;
      min_work_op_index = op_id;
    }
  }
  if (min_work == INT_MAX) {
    min_work = -1;
  }
  return std::make_pair(min_work_op_index, min_work);
}

WorkerMessage* QueryManagerSingleNode::getWorkerMessageFromOperator(
    const dag_node_index index, const numa_node_id numa_node) {
  WorkOrder *work_order = nullptr;
  if (query_exec_state_->hasExecutionFinished(index)) {
    return nullptr;
  }
  if (numa_node != kAnyNUMANodeID) {
    // First try to get a normal WorkOrder from the specified NUMA node.
    work_order =
        workorders_container_->getNormalWorkOrderForNUMANode(index, numa_node);
    if (work_order != nullptr) {
      // A WorkOrder found on the given NUMA node.
      query_exec_state_->incrementNumQueuedWorkOrders(index);
      return WorkerMessage::WorkOrderMessage(work_order, index);
    } else {
      // Normal workorder not found on this node. Look for a rebuild workorder
      // on this NUMA node.
      work_order = workorders_container_->getRebuildWorkOrderForNUMANode(
          index, numa_node);
      if (work_order != nullptr) {
        return WorkerMessage::RebuildWorkOrderMessage(work_order, index);
      }
    }
  }
  // Either no workorder found on the given NUMA node, or numa_node is
  // 'kAnyNUMANodeID'.
  // Try to get a normal WorkOrder from other NUMA nodes.
  work_order = workorders_container_->getNormalWorkOrder(index);
  if (work_order != nullptr) {
    query_exec_state_->incrementNumQueuedWorkOrders(index);
    return WorkerMessage::WorkOrderMessage(work_order, index);
  } else {
    // Normal WorkOrder not found, look for a RebuildWorkOrder.
    work_order = workorders_container_->getRebuildWorkOrder(index);
    if (work_order != nullptr) {
      return WorkerMessage::RebuildWorkOrderMessage(work_order, index);
    }
  }
  return nullptr;
}

WorkerMessage* QueryManagerSingleNode::getNextWorkerMessage(
    const dag_node_index start_operator_index, const numa_node_id numa_node) {
  // We will ignore the start_operator_index argument.
  std::vector<dag_node_index> finished_operators;
  WorkerMessage *msg = nullptr;
  int next_op_id = scheduling_strategy_->getNextOperator();
  if (next_op_id >= 0) {
    msg = getWorkerMessageFromOperator(
        next_op_id, numa_node);
  }
  // else case implies no WorkOrders are available right now.
  return msg;
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
  std::vector<partition_id> part_ids;

  DCHECK(query_context_ != nullptr);
  InsertDestination *insert_destination = query_context_->getInsertDestination(insert_destination_index);
  DCHECK(insert_destination != nullptr);

  insert_destination->getPartiallyFilledBlocks(&partially_filled_block_refs, &part_ids);

  for (std::vector<MutableBlockReference>::size_type i = 0;
       i < partially_filled_block_refs.size();
       ++i) {
    container->addRebuildWorkOrder(
        new RebuildWorkOrder(query_id_,
                             std::move(partially_filled_block_refs[i]),
                             index,
                             op.getOutputRelationID(),
                             part_ids[i],
                             foreman_client_id_,
                             bus_),
        index);
  }
}

std::size_t QueryManagerSingleNode::getQueryMemoryConsumptionBytes() const {
  const std::size_t temp_relations_memory =
      getTotalTempRelationMemoryInBytes();
  const std::size_t temp_data_structures_memory =
      query_context_->getTempStructuresMemoryBytes();
  return temp_relations_memory + temp_data_structures_memory;
}

std::size_t QueryManagerSingleNode::getTotalTempRelationMemoryInBytes() const {
  std::vector<relation_id> temp_relation_ids;
  query_context_->getTempRelationIDs(&temp_relation_ids);
  std::size_t memory = 0;
  for (std::size_t rel_id : temp_relation_ids) {
    if (database_.hasRelationWithId(rel_id)) {
      memory += database_.getRelationById(rel_id)->getRelationSizeBytes();
    }
  }
  return memory;
}

void QueryManagerSingleNode::markOperatorFinished(const dag_node_index index) {
  /*active_operators_.erase(
      std::remove(active_operators_.begin(), active_operators_.end(), index),
      active_operators_.end());
  refillOperators();
  // Reset the next active operator index to 0.
  next_active_op_index_ = 0;*/
  scheduling_strategy_->informCompletionOfOperator(index);

  query_exec_state_->setExecutionFinished(index);

  RelationalOperator *op = query_dag_->getNodePayloadMutable(index);
  op->updateCatalogOnCompletion();

  const relation_id output_rel = op->getOutputRelationID();
  for (const std::pair<dag_node_index, bool> &dependent_link : query_dag_->getDependents(index)) {
    const dag_node_index dependent_op_index = dependent_link.first;
    RelationalOperator *dependent_op = query_dag_->getNodePayloadMutable(dependent_op_index);
    // Signal dependent operator that current operator is done feeding input blocks.
    if (output_rel >= 0) {
      dependent_op->doneFeedingInputBlocks(output_rel);
    }
    if (checkAllBlockingDependenciesMet(dependent_op_index)) {
      dependent_op->informAllBlockingDependenciesMet();
    }
  }
}

bool QueryManagerSingleNode::compareOperatorsUsingRemainingWork(
    dag_node_index a, dag_node_index b) const {
  const std::size_t a_total_wo =
      workorders_container_->getNumTotalWorkOrders(a);
  const std::size_t b_total_wo =
      workorders_container_->getNumTotalWorkOrders(b);
  if (a_total_wo < b_total_wo) {
    return true;
  } else if (a_total_wo == b_total_wo) {
    // Break ties by declaring operator with higher index as higher.
    return a < b;
  } else {
    return false;
  }
}

void QueryManagerSingleNode::printPendingWork() const {
  for (auto i : waiting_operators_) {
    std::cout << i << ":" << workorders_container_->getNumTotalWorkOrders(i) << " ";
  }
  std::cout << std::endl;
}

void QueryManagerSingleNode::refillOperators() {
  switch (FLAGS_scheduling_strategy) {
    case kShortestRemainingWorkFirst: {
      refillOperatorsHelper<true>();
      break;
    }
    case kLargestRemainingWorkFirst: {
      refillOperatorsHelper<false>();
      break;
    }
    case kStaticOrderTopoSort: {
      // Check if the current active operator has finished execution.
      break;
    }
  }
}

void QueryManagerSingleNode::initializeState() {
  next_active_op_index_ = 0;
  switch (FLAGS_scheduling_strategy) {
    case kShortestRemainingWorkFirst: // Fall through
    case kLargestRemainingWorkFirst: {
      // Populate the active operators list.
      for (dag_node_index index = 0; index < num_operators_in_dag_; ++index) {
        waiting_operators_.emplace_back(index);
      }
      break;
    }
    case kStaticOrderTopoSort: {
      // A topologically sorted list of operators in the DAG.
      std::vector<dag_node_index> topo_sorted_operators(
          query_dag_->getTopologicalSorting());
      DCHECK(!topo_sorted_operators.empty());
      // Insert the first element of this vector in active_operators_ and insert
      // others in the waiting_operators_ list.
      active_operators_.emplace_back(topo_sorted_operators.front());
      // Get the second element's iter.
      auto waiting_operators_begin_it = topo_sorted_operators.begin() + 1;
      waiting_operators_.insert(waiting_operators_.end(),
                                waiting_operators_begin_it,
                                topo_sorted_operators.end());
      break;
    }
  }
}

template <bool shortest_remaining_work_first>
void QueryManagerSingleNode::refillOperatorsHelper() {
  const std::size_t original_active_operators_count = active_operators_.size();
  std::size_t num_operators_checked = 0;
  DCHECK_LT(active_operators_.size(), kMaxActiveOperators);
  while (num_operators_checked < waiting_operators_.size() &&
         active_operators_.size() < kMaxActiveOperators) {
    // Get a new candidate operator.
    std::pair<std::size_t, int> next_candidate_for_active_ops(0Lu, 0);
    if (shortest_remaining_work_first) {
      next_candidate_for_active_ops = getLowestWaitingOperatorNonZeroWork();
    } else {
      next_candidate_for_active_ops = getHighestWaitingOperator();
    }
    if (next_candidate_for_active_ops.second >= 0) {
      // There's a candidate. Remove it from the waiting list and insert it
      // in the active list.
      dag_node_index next_op = next_candidate_for_active_ops.first;
      waiting_operators_.erase(
          std::remove(
              waiting_operators_.begin(), waiting_operators_.end(), next_op),
          waiting_operators_.end());
      active_operators_.emplace_back(next_op);
      ++num_operators_checked;
    } else {
      break;
    }
  }
  if (original_active_operators_count != active_operators_.size()) {
    // This means, we added new operators to the active operators' list.
    // Reset the next active operator index to 0.
    next_active_op_index_ = 0;
  }
}

}  // namespace quickstep
