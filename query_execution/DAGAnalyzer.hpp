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

#ifndef QUICKSTEP_QUERY_EXECUTION_DAG_ANALYZER_HPP_
#define QUICKSTEP_QUERY_EXECUTION_DAG_ANALYZER_HPP_

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "query_execution/Pipeline.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "utility/DAG.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup QueryExecution
 *  @{
 */

/**
 * @brief A class that processes a query plan DAG and produces pipelines of
 *        relational operators from the DAG.
 **/
class DAGAnalyzer {
 public:
  /**
   * @brief Constructor. The invocation of the constructor triggers the DAG
   *        analysis and the pipelines are produced.
   *
   * @param query_plan_dag The query plan DAG.
   **/
  explicit DAGAnalyzer(DAG<RelationalOperator, bool> *query_plan_dag)
      : query_plan_dag_(query_plan_dag) {
    findPipelines();
  }

  /**
   * @brief Get the number of pipelines in the DAG.
   **/
  const std::size_t getNumPipelines() const {
    return pipelines_.size();
  }

  /**
   * @brief Get the pipeline ID of the given operator.
   *
   * @param operator_id The ID of the given operator.
   *
   * @return Pipeline ID if such a pipeline exists, otherwise -1.
   **/
  const int getPipelineID(const std::size_t operator_id) const {
    for (std::size_t pipeline_id = 0;
         pipeline_id < pipelines_.size();
         ++pipeline_id) {
      if (pipelines_[pipeline_id]->hasOperator(operator_id)) {
        return pipeline_id;
      }
    }
    return -1;
  }

  /**
   * @brief Check if pipeline has dependencies.
   *
   * @note This is a static check, i.e. it relies on the initial configuration
   *       of the DAG and doesn't care about updates in the DAG.
   *
   * @param pipeline_id The ID of the pipeline.
   *
   * @return True if the pipeline has dependencies, false otherwise.
   **/
  bool checkPipelinehasDependenciesStatic(const std::size_t pipeline_id) const {
    return !getPipelinesConnectedToStart(pipeline_id).empty();
  }

  /**
   * @brief Get the pipelines that are connected to the start of the given
   *        pipeline.
   **/
  std::vector<std::size_t> getPipelinesConnectedToStart(
      const std::size_t pipeline_id) const {
    const std::size_t pipeline_start_node_id =
        pipelines_[pipeline_id]->getPipelineStartPoint();
    auto dependency_nodes = query_plan_dag_->getDependencies(pipeline_start_node_id);
    return getPipelinesForNodes(dependency_nodes);
  }

  /**
   * @brief Get the pipelines that are connected to the end of the given
   *        pipeline.
   **/
  std::vector<std::size_t> getPipelinesConnectedToEnd(
      const std::size_t pipeline_id) const {
    const std::size_t pipeline_end_node_id =
        pipelines_[pipeline_id]->getPipelineEndPoint();
    return getPipelinesForNodes(
        query_plan_dag_->getDependentsAsSet(pipeline_end_node_id));
  }

  /**
   * @brief Get the pipelines that don't have a dependency pipeline.
   *
   * @note This is a static check, performed by disregarding the state of the DAG.
   **/
  std::vector<std::size_t> getFreePipelinesStatic() const {
    std::vector<std::size_t> free_pipelines;
    for (std::size_t id = 0; id < pipelines_.size(); ++id) {
      if (!checkPipelinehasDependenciesStatic(id)) {
        free_pipelines.emplace_back(id);
      }
    }
    return free_pipelines;
  }

  void visualizePipelines();

 private:
  /**
   * @brief Information of a graph node.
   */
  struct NodeInfo {
    std::size_t id;
    std::string labels;
    std::string color;
  };

  /**
   * @brief Information of a graph edge.
   */
  struct EdgeInfo {
    EdgeInfo(std::size_t src, std::size_t dst)
        : src_node_id(src), dst_node_id(dst) {}

    std::size_t src_node_id;
    std::size_t dst_node_id;
  };

  DAG<RelationalOperator, bool> *query_plan_dag_;
  std::vector<std::unique_ptr<Pipeline>> pipelines_;

  /**
   * @brief Find initial set of pipelines in the query plan DAG.
   **/
  void findPipelines();

  /**
   * @brief Find the total number of nodes in all pipelines.
   **/
  const std::size_t getTotalNodes();

  std::vector<std::size_t> getPipelinesForNodes(
      std::unordered_set<std::size_t> node_ids) const {
    if (!node_ids.empty()) {
      std::vector<std::size_t> pipelines;
      for (auto node_id : node_ids) {
        pipelines.emplace_back(getPipelineID(node_id));
      }
      return pipelines;
    }
    return {};
  }

  struct NodeInfo plotSinglePipeline(std::size_t pipeline_id) const;

  std::string visualizePipelinesHelper(const std::vector<struct NodeInfo> &pipelines_info,
                                const std::vector<struct EdgeInfo> &edges);

  DISALLOW_COPY_AND_ASSIGN(DAGAnalyzer);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_DAG_ANALYZER_HPP_
