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

#ifndef QUICKSTEP_QUERY_EXECUTION_QUERY_MANAGER_SINGLE_NODE_HPP_
#define QUICKSTEP_QUERY_EXECUTION_QUERY_MANAGER_SINGLE_NODE_HPP_

#include <cstddef>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/QueryManagerBase.hpp"
#include "query_execution/WorkOrdersContainer.hpp"
#include "utility/Macros.hpp"

#include "tmb/id_typedefs.h"

namespace tmb { class MessageBus; }

namespace quickstep {

class CatalogDatabase;
class CatalogDatabaseLite;
class QueryHandle;
class StorageManager;
class WorkerMessage;

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief A class that manages the execution of a query including generation
 *        of new work orders, keeping track of the query exection state.
 **/
class QueryManagerSingleNode final : public QueryManagerBase {
 public:
  /**
   * @brief Constructor.
   *
   * @param foreman_client_id The TMB client ID of the foreman thread.
   * @param num_numa_nodes The number of NUMA nodes used by the system.
   * @param query_handle The QueryHandle object for this query.
   * @param catalog_database The CatalogDatabse used by the query.
   * @param storage_manager The StorageManager used by the query.
   * @param bus The TMB used for communication.
   **/
  QueryManagerSingleNode(const tmb::client_id foreman_client_id,
                         const std::size_t num_numa_nodes,
                         QueryHandle *query_handle,
                         CatalogDatabaseLite *catalog_database,
                         StorageManager *storage_manager,
                         tmb::MessageBus *bus);

  ~QueryManagerSingleNode() override {}

  bool fetchNormalWorkOrders(const dag_node_index index) override;

 /**
   * @brief Get the next workorder to be excuted, wrapped in a WorkerMessage.
   *
   * @param start_operator_index Begin the search for the schedulable WorkOrder
   *        with the operator at this index.
   * @param numa_node The next WorkOrder should preferably have its input(s)
   *        from this numa_node. This is a hint and not a binding requirement.
   *
   * @return A pointer to the WorkerMessage. If there's no WorkOrder to be
   *         executed, return NULL.
   **/
  WorkerMessage* getNextWorkerMessage(
      const dag_node_index start_operator_index,
      const numa_node_id node_id = kAnyNUMANodeID);

  /**
   * @brief Get a pointer to the QueryContext.
   **/
  inline QueryContext* getQueryContextMutable() {
    return query_context_.get();
  }

  std::size_t getQueryMemoryConsumptionBytes() const override;

 private:
  static constexpr std::size_t kMaxActiveOperators = 1;

  bool checkNormalExecutionOver(const dag_node_index index) const override {
    return (checkAllDependenciesMet(index) &&
            !workorders_container_->hasNormalWorkOrder(index) &&
            query_exec_state_->getNumQueuedWorkOrders(index) == 0 &&
            query_exec_state_->hasDoneGenerationWorkOrders(index));
  }

  bool initiateRebuild(const dag_node_index index) override;

  bool checkRebuildOver(const dag_node_index index) const override {
    return query_exec_state_->hasRebuildInitiated(index) &&
           !workorders_container_->hasRebuildWorkOrder(index) &&
           (query_exec_state_->getNumRebuildWorkOrders(index) == 0);
  }

  /**
   * @brief Get the rebuild WorkOrders for an operator.
   *
   * @note This function should be called only once, when all the normal
   *       WorkOrders generated by an operator finish their execution.
   *
   * @param index The index of the operator in the query plan DAG.
   * @param container A pointer to a WorkOrdersContainer to be used to store the
   *        generated WorkOrders.
   **/
  void getRebuildWorkOrders(const dag_node_index index,
                            WorkOrdersContainer *container);

  /**
   * @brief Get the total memory (in bytes) occupied by temporary relations
   *        created during the query execution.
   **/
  std::size_t getTotalTempRelationMemoryInBytes() const;

  void markOperatorFinished(const dag_node_index index) override;

  bool compareOperatorsUsingRemainingWork(dag_node_index a, dag_node_index b) const;

  std::pair<dag_node_index, int> getHighestWaitingOperator();

  std::pair<dag_node_index, int> getLowestWaitingOperatorNonZeroWork();

  void printPendingWork() const;

  WorkerMessage* getWorkerMessageFromOperator(const dag_node_index index,
                                              const numa_node_id numa_node);

  /**
   * @brief If the number of active operators are below a threshold, refill the
   *        set of active operators.
   **/
  void refillOperators();

  const tmb::client_id foreman_client_id_;

  StorageManager *storage_manager_;
  tmb::MessageBus *bus_;

  std::unique_ptr<QueryContext> query_context_;

  std::unique_ptr<WorkOrdersContainer> workorders_container_;

  const CatalogDatabase &database_;

  std::vector<dag_node_index> waiting_operators_;

  std::vector<dag_node_index> active_operators_;

  // The index of the next active operator to be used for work generation.
  std::size_t next_active_op_index_;

  DISALLOW_COPY_AND_ASSIGN(QueryManagerSingleNode);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_QUERY_MANAGER_SINGLE_NODE_HPP_
