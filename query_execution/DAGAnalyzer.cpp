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

#include "query_execution/DAGAnalyzer.hpp"

#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "utility/DAG.hpp"

#include "glog/logging.h"

namespace quickstep {

void DAGAnalyzer::findPipelines() {
  // Key = node ID, value = whether the node has been visited or not.
  std::unordered_map<std::size_t, bool> visited_nodes;
  // Pair 1st element: Operator ID
  // Pair 2nd element: The index of the pipeline in pipelines_ vector to which
  // the 1st element belongs.
  std::queue<std::pair<std::size_t, std::size_t>> to_be_checked_nodes;
  // Traverse the DAG once. Find leaf nodes and create a pipeline with one node.
  for (std::size_t node_id = 0; node_id < query_plan_dag_->size(); ++node_id) {
    if (query_plan_dag_->getDependencies(node_id).empty()) {
      pipelines_.push_back(std::unique_ptr<Pipeline>(new Pipeline(node_id)));
      to_be_checked_nodes.emplace(node_id, pipelines_.size() - 1);
      // No check needed here, as we are visiting these nodes for the first time.
      visited_nodes[node_id] = true;
    }
  }
  while (!to_be_checked_nodes.empty()) {
    // Get the next node to be checked.
    auto queue_front = to_be_checked_nodes.front();
    to_be_checked_nodes.pop();
    const std::size_t pipeline_id = queue_front.second;
    const std::size_t operator_id = queue_front.first;

    // Make sure this node has already been visited.
    DCHECK(visited_nodes.end() != visited_nodes.find(operator_id));

    // Get all the dependents of the operator.
    // This is a list of pairs, in which first element is the operator ID of the
    // dependent and second element is a boolean.
    // If the boolean is true, that means the link between the operators is
    // pipeline breaker, otherwise it is pipeline-able.
    auto dependents = query_plan_dag_->getDependents(operator_id);
    DLOG(INFO) << "Scanning dependents of " << operator_id << std::endl;

    // Traverse over all the dependents ...
    for (auto dependent_pair : dependents) {
      const std::size_t dependent_id = dependent_pair.first;
      // We create a new pipeline for the dependent if at least one of the two
      // condition holds:
      // 1. There are more than one dependencies of the given node.
      // 2. The edge between two nodes is pipeline-breaking.
      const bool dependent_in_new_pipeline =
          (query_plan_dag_->getDependencies(dependent_id).size() > 1u) ||
          (dependent_pair.second);
      DLOG(INFO) << "Dependent ID: " << dependent_id << " in new pipeline? " << dependent_in_new_pipeline << std::endl;
      if (dependent_in_new_pipeline) {
        // Start a new pipeline.
        if (visited_nodes.find(dependent_id) == visited_nodes.end()) {
          pipelines_.push_back(std::unique_ptr<Pipeline>(new Pipeline(dependent_id)));
          to_be_checked_nodes.emplace(dependent_id, pipelines_.size() - 1);
          // Link the two pipelines. dependent_id is the dependent of
          // operator_id, which means the new pipeline is a dependent of the
          // old pipeline.
          pipelines_[pipeline_id]->linkPipeline(
              pipelines_.size() - 1, operator_id, true);
          pipelines_[pipelines_.size() - 1]->linkPipeline(
              pipeline_id, dependent_id, false);
          visited_nodes[dependent_id] = true;
          DLOG(INFO) << "New pipeline for dependent " << dependent_id << std::endl;
        } else {
          // We should find the pipelines of which dependent_id is a member.
          auto pipelines_containing_dependent = getPipelineID(dependent_id);
          // As dependent is in new pipeline, we are assured that we can connect
          // pipelines in "pipelines_containing_dependent" with the current
          // pipeline.
          DLOG(INFO) << "Dependent " << dependent_id << " already visited" << std::endl;
          for (auto i : pipelines_containing_dependent) {
            DLOG(INFO) << "Pipeline (ID="<< pipeline_id << ") containing " << operator_id << " linked with pipeline (ID=" << i << "containing " << dependent_id << std::endl;
            pipelines_[pipeline_id]->linkPipeline(
                i, operator_id, true);
          }
        }
      } else {
        // This means that pipelining is enabled on this link. Add this
        // dependent to the current pipeline being processed.
        if (visited_nodes.find(dependent_id) == visited_nodes.end()) {
          pipelines_[pipeline_id]->addOperatorToPipeline(dependent_id);
          to_be_checked_nodes.emplace(dependent_id, pipeline_id);
          visited_nodes[dependent_id] = true;
          DLOG(INFO) << "Dependent " << dependent_id << " appended to pipeline " << pipeline_id << std::endl;
        } else {
          DLOG(INFO) << "Dependent " << dependent_id << " already visited" << std::endl;
        }
      }
    }

    // Now get the dependencies of the given node.
    // If the dependency node and the given node belong to different pipelines
    // then link these two pipelines.
    /*auto dependencies = query_plan_dag_->getDependencies(operator_id);
    bool more_than_one_dependencies = (dependencies.size() > 1u);
    for (auto dependency : dependencies) {
      if (!more_than_one_dependencies) {
        // Therefore exactly one dependency.
        if (query_plan_dag_->getLinkMetadata(dependency, operator_id)) {
          // This is a pipeline breaker link, which means dependency and operator_id
          // belong to different pipelines.
        }
      }
      given_operator_in_new_pipeline =
          query_plan_dag_->getLinkMetadata(dependency, operator_id);
      if (given_operator_in_new_pipeline) {
        // Confirm if there are more than one dependencies for operator.
      }
      auto pipelines_ids_containing_dependency = getPipelineID(dependency);
      removeElementFromVector(&pipelines_ids_containing_dependency, pipeline_id);
      for (auto curr_pipeline_id_containing_dependency :
           pipelines_ids_containing_dependency) {
      }
    }*/
  }
  // Make sure that all nodes belong to some pipeline exactly once.
  DCHECK_EQ(query_plan_dag_->size(), getTotalNodes());
}

const std::size_t DAGAnalyzer::getTotalNodes() {
  std::size_t total = 0;
  for (std::size_t i = 0; i < pipelines_.size(); ++i) {
    total += pipelines_[i]->size();
  }
  return total;
}

void DAGAnalyzer::visualizePipelines() {
  std::vector<struct NodeInfo> pipelines_info;
  std::vector<struct EdgeInfo> edges;
  for (std::size_t id = 0; id < pipelines_.size(); ++id) {
    plotSinglePipeline(id, &pipelines_info, &edges);
  }
  std::cout << "# nodes: " << pipelines_info.size() << " # edges: " << edges.size() << std::endl;
  std::cout << visualizePipelinesHelper(pipelines_info, edges) << std::endl;
}

std::string DAGAnalyzer::visualizePipelinesHelper(
    const std::vector<struct NodeInfo> &pipelines_info,
    const std::vector<struct EdgeInfo> &edges) {
  // Format output graph
  std::ostringstream graph_oss;
  graph_oss << "strict digraph g {\n";
  graph_oss << "  rankdir=BT\n";
  graph_oss << "  node [penwidth=2]\n";
  graph_oss << "  edge [fontsize=16 fontcolor=gray penwidth=2]\n\n";

  // Format nodes
  for (const auto &pipeline : pipelines_info) {
    graph_oss << pipeline.id << " [ label= \"";
    graph_oss << pipeline.labels;
    graph_oss << "\"]\n";
  }

  // Format edges
  for (const EdgeInfo &edge_info : edges) {
    graph_oss << "  " << edge_info.src_node_id << " -> "
              << edge_info.dst_node_id << " [ ";
    graph_oss << "]\n";
  }
  graph_oss << "}\n";
  return graph_oss.str();
}

void DAGAnalyzer::plotSinglePipeline(
    std::size_t pipeline_id,
    std::vector<struct NodeInfo> *nodes,
    std::vector<struct EdgeInfo> *edges) const {
  NodeInfo current_node;
  current_node.id = pipeline_id;
  current_node.labels += "[";
  current_node.labels += std::to_string(pipeline_id);
  current_node.labels += "] Operators: ";
  auto nodes_in_pipeline = pipelines_[pipeline_id]->getOperatorIDs();
  for (auto node_id : nodes_in_pipeline) {
    current_node.labels += std::to_string(node_id);
    current_node.labels += "  ";
  }
  current_node.labels += "";
  nodes->push_back(current_node);
  auto neighbor_pipelines = pipelines_[pipeline_id]->getAllConnectedPipelines();
  for (const PipelineConnection &neighbor : neighbor_pipelines) {
    if (neighbor.checkPipelineIsDependent()) {
      edges->emplace_back(pipeline_id, neighbor.getConnectedPipelineID());
    } else {
      edges->emplace_back(neighbor.getConnectedPipelineID(), pipeline_id);
    }
  }
}

}  // namespace quickstep
