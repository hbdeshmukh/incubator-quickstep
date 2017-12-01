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

#ifndef QUICKSTEP_QUERY_EXECUTION_PIPELINE_SCHEDULING_HPP
#define QUICKSTEP_QUERY_EXECUTION_PIPELINE_SCHEDULING_HPP

#include <cstddef>
#include <queue>
#include <vector>

#include "query_execution/IntraQuerySchedulingStrategy.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "utility/DAG.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class DAGAnalyzer;
class QueryExecutionState;
class WorkOrdersContainer;

class PipelineScheduling : public IntraQuerySchedulingStrategy {
 public:
  /**
   * @brief Constructor
   * @param query_dag The query plan DAG
   * @param dag_analyzer The DAG analyzer
   * @param workorders_container The WorkOrdersContainer object for this query.
   * @param query_exec_state The QueryExecutionState object.
   */
  PipelineScheduling(DAG<RelationalOperator, bool> *query_dag,
                     const DAGAnalyzer *dag_analyzer,
                     WorkOrdersContainer *workorders_container,
                     const QueryExecutionState &query_exec_state);

  ~PipelineScheduling() override {
  }

  int getNextOperator() override;

  void informCompletionOfOperator(std::size_t operator_index) override;

 private:
  bool isPipelineExecutionOver(std::size_t pipeline_id) const;

  const DAG<RelationalOperator, bool> *query_dag_;
  const DAGAnalyzer *dag_analyzer_;

  WorkOrdersContainer *container_;

  const QueryExecutionState &query_exec_state_;

  std::queue<std::size_t> pipelines_not_started_;

  std::vector<std::size_t> running_pipelines_;

  DISALLOW_COPY_AND_ASSIGN(PipelineScheduling);
};

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_PIPELINE_SCHEDULING_HPP
