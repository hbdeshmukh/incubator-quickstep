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
   * @brief Get the IDs of those pipelines of which operator_id is a part.
   *
   * @param operator_id The ID of the given operator.
   *
   * @return Pipeline IDs if such pipeline(s) exist.
   **/
  const std::vector<std::size_t> getPipelineID(const std::size_t operator_id) const {
    std::vector<std::size_t> result_pipelines;
    for (std::size_t pipeline_id = 0;
         pipeline_id < pipelines_.size();
         ++pipeline_id) {
      if (pipelines_[pipeline_id]->hasOperator(operator_id)) {
        result_pipelines.emplace_back(pipeline_id);
      }
    }
    return result_pipelines;
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
  std::unordered_set<std::size_t> getPipelinesConnectedToStart(
      const std::size_t pipeline_id) const {
    const std::size_t pipeline_start_node_id =
        pipelines_[pipeline_id]->getPipelineStartPoint();
    auto dependency_nodes = query_plan_dag_->getDependencies(pipeline_start_node_id);
    auto pipelines_containing_dependency_nodes =
        getPipelinesForNodes(dependency_nodes);
    // Remove self pipeline_id from this set.
    removeElementFromSet(&pipelines_containing_dependency_nodes, pipeline_id);
    return pipelines_containing_dependency_nodes;
  }

  /**
   * @brief Get the pipelines that are connected to the end of the given
   *        pipeline.
   **/
  std::unordered_set<std::size_t> getPipelinesConnectedToEnd(
      const std::size_t pipeline_id) const {
    const std::size_t pipeline_end_node_id =
        pipelines_[pipeline_id]->getPipelineEndPoint();
    auto pipelines_containing_dependent_nodes = getPipelinesForNodes(
        query_plan_dag_->getDependentsAsSet(pipeline_end_node_id));
    // Remove self pipeline_id from this set.
    removeElementFromSet(&pipelines_containing_dependent_nodes, pipeline_id);
    return pipelines_containing_dependent_nodes;
  }

  /**
   * @brief Get the pipelines which are connected (and end) at an intermediate
   *        node of the given pipeline.
   *
   * @note This function is only relevant if there exists an intermediate node
   *       in the pipeline, i.e. len(pipeline) > 2.
   *
   * @note Because of the condition that "a node with more than one dependencies
   *       should belong to its own pipeline", this function will always result
   *       empty vector.
   **/
  std::unordered_set<std::size_t> getPipelinesEndingAtIntermediateNode(
      const std::size_t pipeline_id) {
    std::unordered_set<std::size_t> result_pipelines;
    return result_pipelines;
  }

  /**
   * @brief Get the pipelines which are connected (and begin ) at an
   *        intermediate node of the given pipeline.
   *
   * @note This function is only relevant if there exists an intermediate node
   *       in the pipeline, i.e. len(pipeline) > 2.
   **/
  std::unordered_set<std::size_t> getPipelinesStartingAtIntermediateNode(
      const std::size_t pipeline_id) {
    std::unordered_set<std::size_t> result_pipelines;
    if (pipelines_[pipeline_id]->size() > 2u) {
      // Find the intermediate nodes.
      auto all_nodes = pipelines_[pipeline_id]->getOperatorIDs();
      std::unordered_set<std::size_t> intermediate_nodes;
      auto begin_it = all_nodes.begin() + 1;
      auto end_it = all_nodes.end() - 1;
      intermediate_nodes.insert(begin_it, end_it);
      result_pipelines = getPipelinesForNodes(intermediate_nodes);
      removeElementFromSet(&result_pipelines, pipeline_id);
    }
    return result_pipelines;
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

  const std::vector<std::size_t> getAllOperatorsInPipeline(
      const std::size_t pipeline_id) const {
    DCHECK_LT(pipeline_id, pipelines_.size());
    return pipelines_[pipeline_id]->getOperatorIDs();
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
    EdgeInfo(std::size_t src, std::size_t dst, bool can_edges_be_fused)
        : src_node_id(src),
          dst_node_id(dst),
          can_be_fused(can_edges_be_fused) {}

    std::size_t src_node_id;
    std::size_t dst_node_id;
    bool can_be_fused;
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

  std::unordered_set<std::size_t> getPipelinesForNodes(
      std::unordered_set<std::size_t> node_ids) const {
    std::unordered_set<std::size_t> pipelines;
    if (!node_ids.empty()) {
      for (auto node_id : node_ids) {
        auto pipelines_containing_node_id = getPipelineID(node_id);
        pipelines.insert(pipelines_containing_node_id.begin(),
                         pipelines_containing_node_id.end());
      }
    }
    return pipelines;
  }

  void removeElementFromVector(std::vector<std::size_t> *vec,
                               std::size_t elem_removed) const {
    auto it = std::find(vec->begin(), vec->end(), elem_removed);
    if (it != vec->end()) {
      vec->erase(it);
    }
  }

  void removeElementFromSet(std::unordered_set<std::size_t> *s,
                            std::size_t elem_removed) const {
    auto it = std::find(s->begin(), s->end(), elem_removed);
    if (it != s->end()) {
      s->erase(it);
    }
  }

  void plotSinglePipeline(std::size_t pipeline_id,
                          std::vector<struct NodeInfo> *nodes,
                          std::vector<struct EdgeInfo> *edges) const;

  std::string visualizePipelinesHelper(const std::vector<struct NodeInfo> &pipelines_info,
                                const std::vector<struct EdgeInfo> &edges);

  /**
   * @brief Check if src pipeline be fused with dst pipeline.
   **/
  bool canPipelinesBeFused(const std::size_t src_pipeline_id,
                           const std::size_t dst_pipeline_id) const;

  DISALLOW_COPY_AND_ASSIGN(DAGAnalyzer);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_EXECUTION_DAG_ANALYZER_HPP_
