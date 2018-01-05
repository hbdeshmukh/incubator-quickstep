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

#ifndef QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_INTRA_PIPELINE_SCHEDULING_HPP_
#define QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_INTRA_PIPELINE_SCHEDULING_HPP_

#include <cstddef>
#include <vector>

#include "utility/Macros.hpp"

namespace quickstep {

class DAGAnalyzer;
class QueryExecutionState;
class WorkOrdersContainer;

namespace intra_pipeline {

/** \addtogroup QueryExecution
 *  @{
 */
class IntraPipelineScheduling {
 public:
  /**
   * @brief Constructor
   * @param dag_analyzer The DAGAnalyzer object.
   */
  IntraPipelineScheduling(const QueryExecutionState &query_exec_state,
                          const DAGAnalyzer *dag_analyzer,
                          WorkOrdersContainer *workorders_container) {}

  virtual ~IntraPipelineScheduling() {}

  /**
   * @brief Get the operator for the next scheduling decision.
   */
  virtual int getNextOperator() = 0;

  /**
   * @brief Signal the completion of the given pipeline.
   * @param pipeline_id The ID of the completed pipeline.
   */
  virtual void signalCompletionOfPipeline(std::size_t pipeline_id) = 0;

  /**
   * @brief Signal the start of pipelines.
   * @param pipelines A list of the pipelines.
   *
   * @note If there are more than one pipelines, they are assumed to be fusable.
   * @note The pipelines are assumed to be in bottom to top order in the vector.
   */
  virtual void signalStartOfPipelines(const std::vector<std::size_t> &pipelines) = 0;

  /**
   * @brief Get the currently running pipelines.
   */
  virtual const std::vector<std::size_t>& getRunningPipelines() const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(IntraPipelineScheduling);
};

/** @} */

}  // namespace intra_pipeline
}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_INTRA_PIPELINE_SCHEDULING_INTRA_PIPELINE_SCHEDULING_HPP_
