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
#include <numeric>
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
      dag_analyzer_(new DAGAnalyzer(query_dag_)),
      active_pipelines_(new ActivePipelinesManager(dag_analyzer_.get())) {
  dag_analyzer_->visualizePipelines();
  // Collect all the workorders from all the relational operators in the DAG.
  for (dag_node_index index = 0; index < num_operators_in_dag_; ++index) {
    if (checkAllBlockingDependenciesMet(index)) {
      query_dag_->getNodePayloadMutable(index)->informAllBlockingDependenciesMet();
      processOperator(index, false);
    }
  }
}

WorkerMessage* QueryManagerSingleNode::getNextWorkerMessage(
    const dag_node_index start_operator_index, const numa_node_id numa_node) {
  // Default policy: Operator with lowest index first.
  size_t num_operators_checked = 0;
  for (dag_node_index index = start_operator_index;
       num_operators_checked < num_operators_in_dag_;
       index = (index + 1) % num_operators_in_dag_, ++num_operators_checked) {
    WorkerMessage* next_worker_message = getNextWorkerMessageHelper(index, numa_node);
    if (next_worker_message != nullptr) {
      return next_worker_message;
    }
  }
  // No WorkOrders available right now.
  return nullptr;
}

WorkerMessage* QueryManagerSingleNode::getNextWorkerMessageHelper(
    const std::size_t operator_index, const numa_node_id numa_node) {
  WorkOrder *work_order = nullptr;
  if (!query_exec_state_->hasExecutionFinished(operator_index)) {
    if (numa_node != kAnyNUMANodeID) {
      // First try to get a normal WorkOrder from the specified NUMA node.
      work_order = workorders_container_->getNormalWorkOrderForNUMANode(
          operator_index, numa_node);
      if (work_order != nullptr) {
        // A WorkOrder found on the given NUMA node.
        query_exec_state_->incrementNumQueuedWorkOrders(operator_index);
        return WorkerMessage::WorkOrderMessage(work_order, operator_index);
      } else {
        // Normal workorder not found on this node. Look for a rebuild workorder
        // on this NUMA node.
        work_order = workorders_container_->getRebuildWorkOrderForNUMANode(
            operator_index, numa_node);
        if (work_order != nullptr) {
          return WorkerMessage::RebuildWorkOrderMessage(work_order, operator_index);
        }
      }
    }
    // Either no workorder found on the given NUMA node, or numa_node is
    // 'kAnyNUMANodeID'.
    // Try to get a normal WorkOrder from other NUMA nodes.
    work_order = workorders_container_->getNormalWorkOrder(operator_index);
    if (work_order != nullptr) {
      query_exec_state_->incrementNumQueuedWorkOrders(operator_index);
      return WorkerMessage::WorkOrderMessage(work_order, operator_index);
    } else {
      // Normal WorkOrder not found, look for a RebuildWorkOrder.
      work_order = workorders_container_->getRebuildWorkOrder(operator_index);
      if (work_order != nullptr) {
        return WorkerMessage::RebuildWorkOrderMessage(work_order, operator_index);
      }
    }
  }
  // Either no WorkOrders available right now or the operator has finished its
  // execution.
  return nullptr;
}

WorkerMessage* QueryManagerSingleNode::getNextWorkerMessagePipelineBased(
    const numa_node_id node_id) {
  // TODO(harshad) - Try to get rid of the following call to update active
  // pipelines and instead update the pipelines, only when really needed.
  const std::size_t num_active_pipelines = updateActivePipelines();
  for (std::size_t attempt_num = 0;
       attempt_num < num_active_pipelines;
       ++attempt_num) {
    std::pair<std::size_t, std::size_t> next_pipe_op_pair =
        active_pipelines_->getNextPipelineAndOperatorID();
    WorkerMessage *next_worker_message =
        getNextWorkerMessageHelper(next_pipe_op_pair.second, node_id);
    if (next_worker_message != nullptr) {
      return next_worker_message;
    }
  }
  return nullptr;
}

std::size_t QueryManagerSingleNode::updateActivePipelines() {
  for (std::size_t curr_pipeline_id = 0;
       curr_pipeline_id < dag_analyzer_->getNumPipelines();
       ++curr_pipeline_id) {
    if (isPipelineSchedulable(curr_pipeline_id) &&
        !active_pipelines_->hasPipeline(curr_pipeline_id)) {
      std::cout << "Adding pipeline: " << curr_pipeline_id
                << " Active pipelines: "
                << active_pipelines_->getNumActivePipelines() << "\n";
      active_pipelines_->addPipeline(curr_pipeline_id);
    }
  }
  return active_pipelines_->getNumActivePipelines();
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

bool QueryManagerSingleNode::isPipelineExecutionOver(
    const std::size_t pipeline_id) const {
  for (auto operator_id :
       dag_analyzer_->getAllOperatorsInPipeline(pipeline_id)) {
    if (!query_exec_state_->hasExecutionFinished(operator_id)) {
      return false;
    }
  }
  return true;
}

bool QueryManagerSingleNode::isPipelineSchedulable(
    const std::size_t pipeline_id) const {
  // First check if the pipelines has any dependencies.
  if (isPipelineExecutionOver(pipeline_id)) {
    return false;
  } else {
    // Otherwise, check if all the dependencies of this pipeline have been
    // executed.
    auto dependencies = dag_analyzer_->getAllBlockingDependencies(pipeline_id);
    for (auto dep : dependencies) {
      if (!isPipelineExecutionOver(dep)) {
        return false;
      }
    }
    return true;
  }
}

void QueryManagerSingleNode::markOperatorFinished(const dag_node_index index) {
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
  // Check if the pipeline of which index is a part, has finished its execution.
  auto pipeline_ids_containing_index = dag_analyzer_->getPipelineID(index);
  for (std::size_t pid : pipeline_ids_containing_index) {
    if (isPipelineExecutionOver(pid) && active_pipelines_->hasPipeline(pid)) {
      // Remove pipeline from active pipelines.
      active_pipelines_->removePipeline(pid);
      std::cout << "Removed pipeline: " << pid << " Active pipelines: " << active_pipelines_->getNumActivePipelines() << "\n";
    }
  }
}

}  // namespace quickstep
