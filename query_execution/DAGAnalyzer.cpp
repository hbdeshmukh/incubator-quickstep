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

#include "gflags/gflags.h"
#include "glog/logging.h"

namespace quickstep {

DEFINE_bool(show_all_nodes,
            true,
            "Show both essential and non-essential types of nodes in the "
            "visualization of the pipelines");

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
          const bool can_pipelines_be_fused =
              canPipelinesBeFused(pipeline_id, pipelines_.size() - 1);
          pipelines_[pipeline_id]->linkPipeline(
              pipelines_.size() - 1,
              operator_id,
              true,
              can_pipelines_be_fused);
          pipelines_[pipelines_.size() - 1]->linkPipeline(
              pipeline_id,
              dependent_id,
              false,
              can_pipelines_be_fused);
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
            DLOG(INFO) << "Pipeline (ID=" << pipeline_id << ") containing "
                       << operator_id << " linked with pipeline (ID=" << i
                       << "containing " << dependent_id << std::endl;
            const bool can_pipelines_be_fused =
                canPipelinesBeFused(pipeline_id, i);
            pipelines_[pipeline_id]->linkPipeline(
                i, operator_id, true, can_pipelines_be_fused);
            pipelines_[i]->linkPipeline(
                pipeline_id, operator_id, false, can_pipelines_be_fused);
          }
        }
      } else {
        // This means that pipelining is enabled on this link. Add this
        // dependent to the current pipeline being processed.
        if (visited_nodes.find(dependent_id) == visited_nodes.end()) {
          pipelines_[pipeline_id]->addOperatorToPipeline(dependent_id);
          to_be_checked_nodes.emplace(dependent_id, pipeline_id);
          visited_nodes[dependent_id] = true;
          DLOG(INFO) << "Dependent " << dependent_id << " appended to pipeline "
                     << pipeline_id << std::endl;
        } else {
          DLOG(INFO) << "Dependent " << dependent_id << " already visited"
                     << std::endl;
        }
      }
    }
  }
  // It will be nice to have a function that can sort the pipelines so that the
  // order of operator IDs in each pipeline is A, B, C if in the DAG, the operators
  // appear as A->B->C.
  // Make sure that all nodes belong to some pipeline exactly once.
  DCHECK_EQ(query_plan_dag_->size(), getTotalNodes());
  markEssentialNodes();
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
  std::cout << visualizePipelinesHelper(pipelines_info, edges) << std::endl;
}

bool DAGAnalyzer::checkDisplayPipelineNode(const std::size_t pipeline_id) const {
  using ROEnumType =
  typename std::underlying_type<RelationalOperator::OperatorType>::type;

  // Do not display these relational operators in the graph.
  const std::unordered_set<ROEnumType> no_display_op_types =
      {RelationalOperator::kDestroyAggregationState,
       RelationalOperator::kDestroyHash,
       RelationalOperator::kDropTable};
      //{RelationalOperator::kSample};
  if (pipelines_[pipeline_id]->size() == 1) {
    const ROEnumType operator_type
        = query_plan_dag_->getNodePayload(pipelines_[pipeline_id]->getOperatorIDs().front()).getOperatorType();
    return no_display_op_types.find(operator_type) == no_display_op_types.end();
  } else {
    // Always display a pipeline node with more than one operators.
    return true;
  }
}

bool DAGAnalyzer::isEssentialNode(const std::size_t pipeline_id) const {
  if (pipelines_[pipeline_id]->size() == 1) {
    using ROEnumType =
    typename std::underlying_type<RelationalOperator::OperatorType>::type;

    // Do not display these relational operators in the graph.
    const std::unordered_set<ROEnumType> no_display_op_types =
        {RelationalOperator::kDestroyAggregationState,
         RelationalOperator::kDestroyHash,
         RelationalOperator::kDropTable};

    const ROEnumType operator_type
        = query_plan_dag_->getNodePayload(pipelines_[pipeline_id]->getOperatorIDs().front()).getOperatorType();
    return no_display_op_types.find(operator_type) == no_display_op_types.end();
  }
  // Always display a pipeline node with more than one operators.
  return true;
}

void DAGAnalyzer::markEssentialNodes() {
  for (std::size_t pid = 0; pid < pipelines_.size(); ++pid) {
    if (isEssentialNode(pid)) {
      pipelines_[pid]->markEssential();
    }
  }
}

std::string DAGAnalyzer::visualizePipelinesHelper(
    const std::vector<struct NodeInfo> &pipelines_info,
    const std::vector<struct EdgeInfo> &edges) {
  // Format output graph
  std::ostringstream graph_oss;
  graph_oss << "strict digraph g {\n";
  graph_oss << "  rankdir=BT\n";
  graph_oss << "  node [penwidth=2]\n";
  graph_oss << "  edge [fontsize=12 fontcolor=gray penwidth=2]\n\n";

  // Format nodes
  for (const auto &pipeline : pipelines_info) {
    if (FLAGS_show_all_nodes || checkDisplayPipelineNode(pipeline.id)) {
      graph_oss << pipeline.id << " [ label= \"";
      graph_oss << pipeline.labels;
      graph_oss << "\"]\n";
    }
  }

  // Format edges
  for (const EdgeInfo &edge_info : edges) {
    if (FLAGS_show_all_nodes ||
        (checkDisplayPipelineNode(edge_info.src_node_id) &&
         checkDisplayPipelineNode(edge_info.dst_node_id))) {
      graph_oss << "  " << edge_info.src_node_id << " -> "
                << edge_info.dst_node_id << " [ ";
      if (edge_info.can_be_fused) {
        // Pipelining is allowed over this edge.
        graph_oss << "style=filled";
      } else {
        graph_oss << "style=dashed";
      }
      graph_oss << "]\n";
    }
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
  current_node.labels += "] ";
  auto nodes_in_pipeline = pipelines_[pipeline_id]->getOperatorIDs();
  const std::size_t fixed_len_operator = std::string("Operator").length();
  for (auto node_id : nodes_in_pipeline) {
    const std::string op_string = query_plan_dag_->getNodePayload(node_id).getName();

    if (op_string.find("Operator")) {
      const std::size_t len = query_plan_dag_->getNodePayload(node_id).getName().length();
      const std::string op_display_name =
          query_plan_dag_->getNodePayload(node_id).getName().substr(0, len - fixed_len_operator);
      current_node.labels += std::to_string(node_id) + ":";
      current_node.labels += op_display_name;
    } else {
      current_node.labels += op_string;
    }
    current_node.labels += "-";
  }
  nodes->push_back(current_node);
  auto neighbor_pipelines = pipelines_[pipeline_id]->getAllConnectedPipelines();
  for (const PipelineConnection &neighbor : neighbor_pipelines) {
    if (neighbor.checkConnectedPipelineIsDependent()) {
      edges->emplace_back(pipeline_id,
                          neighbor.getConnectedPipelineID(),
                          neighbor.canPipelinesBeFused());
    } else {
      edges->emplace_back(neighbor.getConnectedPipelineID(),
                          pipeline_id,
                          neighbor.canPipelinesBeFused());
    }
  }
}

bool DAGAnalyzer::canPipelinesBeFused(const std::size_t src_pipeline_id,
                                      const std::size_t dst_pipeline_id) const {
  const std::size_t src_end_operator_id =
      pipelines_[src_pipeline_id]->getPipelineEndPoint();
  const std::size_t dst_start_operator_id =
      pipelines_[dst_pipeline_id]->getPipelineStartPoint();
  if (query_plan_dag_->hasLink(src_end_operator_id, dst_start_operator_id)) {
    const bool is_pipeline_breaker =
        query_plan_dag_->getLinkMetadata(src_end_operator_id, dst_start_operator_id);
    DLOG(INFO) << "Link between " << src_end_operator_id << " and "
               << dst_start_operator_id << " is pipeline breaker? " << is_pipeline_breaker << std::endl;
    return !is_pipeline_breaker;
  } else {
    DLOG(INFO) << "No Link between " << src_end_operator_id << " and " << dst_start_operator_id << std::endl;
    return false;
  }
}

void DAGAnalyzer::generateEssentialPipelineSequence(
    const std::size_t topmost_pipeline_id, std::vector<size_t> *sequence) const {
  std::vector<PipelineConnection> children(pipelines_[topmost_pipeline_id]->getAllIncomingPipelines());
  if (children.size() == 1u) {
    generateEssentialPipelineSequence(children.front().getConnectedPipelineID(), sequence);
  } else if (children.size() > 1u){
    // sort children from earliest to latest.
    std::sort(children.begin(), children.end(), [&](const PipelineConnection &a, const PipelineConnection &b){
      const std::size_t a_end_point_operator_id = pipelines_[a.getConnectedPipelineID()]->getPipelineEndPoint();
      const std::size_t b_end_point_operator_id = pipelines_[b.getConnectedPipelineID()]->getPipelineEndPoint();
      const RelationalOperator::OperatorType
          a_end_op_type = query_plan_dag_->getNodePayload(a_end_point_operator_id).getOperatorType();
      const RelationalOperator::OperatorType
          b_end_op_type = query_plan_dag_->getNodePayload(b_end_point_operator_id).getOperatorType();
      if (a_end_op_type != RelationalOperator::OperatorType::kBuildHash &&
          b_end_op_type == RelationalOperator::OperatorType::kBuildHash) {
        return false;
      } else if (a_end_op_type == RelationalOperator::OperatorType::kBuildHash &&
                 b_end_op_type != RelationalOperator::OperatorType::kBuildHash) {
        return true;
      } else {
        // Execute operator with lower ID earlier.
        return a_end_point_operator_id < b_end_point_operator_id;
      }
    });
    for (auto child : children) {
      generateEssentialPipelineSequence(child.getConnectedPipelineID(), sequence);
    }
  }
  if (std::find(sequence->begin(), sequence->end(), topmost_pipeline_id) == sequence->end()) {
    /* We need this check for some patterns like below.
      Two pipelines have a sibling relationship (b and c), but they also have
     parent-child relationship.
       PID-a
        /  \
       /   PID-b
      /   /
     PID-c
    */
    sequence->push_back(topmost_pipeline_id);
  }
}

std::vector<std::size_t> DAGAnalyzer::generateEssentialPipelines() const {
  std::vector<std::size_t> essential_pipelines;
  for (std::size_t pid = 0; pid < pipelines_.size(); ++pid) {
    if (pipelines_[pid]->isEssential()) {
      essential_pipelines.emplace_back(pid);
    }
  }
  rearrangeEssentialPipelines(&essential_pipelines);
  return essential_pipelines;
}

std::size_t DAGAnalyzer::getEssentialOutPipelinesCount(const std::size_t pipeline_id) const {
  std::size_t ret_count = 0;
  for (const PipelineConnection &pc : pipelines_[pipeline_id]->getAllOutgoingPipelines()) {
    if (pipelines_[pc.getConnectedPipelineID()]->isEssential()) {
      ++ret_count;
    }
  }
  return ret_count;
}

void DAGAnalyzer::rearrangeEssentialPipelines(std::vector<std::size_t> *essential_pipelines) const {
  // It is easier to find pipelines which don't have any incoming dependencies.
  std::sort(essential_pipelines->begin(), essential_pipelines->end(), [&](const std::size_t a, const std::size_t b) {
    // Count the number of essential pipelines which are "out" of a.
    const std::size_t essential_out_of_a = getEssentialOutPipelinesCount(a);
    const std::size_t essential_out_of_b = getEssentialOutPipelinesCount(b);
    if (essential_out_of_a < essential_out_of_b) {
      return true;
    } else if (essential_out_of_a == essential_out_of_b) {
      return a < b;
    } else {
      return false;
    }
  });
}

std::vector<std::size_t> DAGAnalyzer::generateFinalPipelineSequence() {
  // Key = pipeline ID, value = # incoming pipelines.
  std::unordered_map<std::size_t, std::size_t> incoming_pipelines_count;
  for (std::size_t pid = 0; pid < pipelines_.size(); ++pid) {
    incoming_pipelines_count[pid] = pipelines_[pid]->getAllIncomingPipelines().size();
  }
  std::vector<bool> visited(pipelines_.size(), false);
  std::vector<std::size_t> essential_pipelines(generateEssentialPipelines());
  std::vector<std::size_t> essential_pipelines_list;
  DCHECK(!essential_pipelines.empty());
  generateEssentialPipelineSequence(essential_pipelines.front(), &essential_pipelines_list);
  std::vector<std::size_t> essential_pipelines_list_copy = essential_pipelines_list;
  // Mark the essential pipelines as "visited".
  essential_pipeline_sequence_.insert(
      essential_pipeline_sequence_.end(), essential_pipelines_list.begin(), essential_pipelines_list.end());
  for (std::size_t p : essential_pipelines_list) {
    visited[p] = true;
  }
  std::vector<std::size_t> final_pipeline_sequence;
  for (std::size_t curr_pid : essential_pipelines_list) {
    std::vector<std::size_t> dependent_pipelines;
    dependent_pipelines.emplace_back(curr_pid);
    populateDependents(curr_pid, &dependent_pipelines, &incoming_pipelines_count, &visited);
    // NOTE(harshad) - As we use incoming_pipelines_count as a way to record
    // "visited" pipeline nodes, we set the value for curr_pid to be 0.
    incoming_pipelines_count[curr_pid] = 0u;
    final_pipeline_sequence.insert(final_pipeline_sequence.end(),
                                   dependent_pipelines.begin(),
                                   dependent_pipelines.end());
  }
  return final_pipeline_sequence;
}

void DAGAnalyzer::populateDependents(
    const std::size_t pipeline_id,
    std::vector<std::size_t> *final_sequence,
    std::unordered_map<std::size_t, std::size_t> *incoming_pipelines_count,
    std::vector<bool> *visited)
    const {
  std::vector<PipelineConnection> outgoing_pipelines =
      pipelines_[pipeline_id]->getAllOutgoingPipelines();
  std::queue<std::size_t> outgoing_pipelines_queue;
  for (auto pc : outgoing_pipelines) {
    outgoing_pipelines_queue.push(pc.getConnectedPipelineID());
  }
  while (!outgoing_pipelines_queue.empty()) {
    const std::size_t connected_pid = outgoing_pipelines_queue.front();
    outgoing_pipelines_queue.pop();
    if ((*visited)[connected_pid]) {
      continue;
    }
    // NOTE(harshad) - This pipeline is assumed to have only one operator,
    // as these pipelines are for operators like DestroyHash, DropTable.
    if (pipelines_[connected_pid]->size() == 1u) {
      if ((*incoming_pipelines_count)[connected_pid] >= 1u) {
        (*incoming_pipelines_count)[connected_pid] -= 1;
        if ((*incoming_pipelines_count)[connected_pid] == 0u) {
          final_sequence->emplace_back(connected_pid);
          for (auto pc : pipelines_[connected_pid]->getAllOutgoingPipelines()) {
            outgoing_pipelines_queue.push(pc.getConnectedPipelineID());
          }
        }
      }
    }
  }
}

void DAGAnalyzer::createOperatorToPipelineLookup() {
  for (std::size_t pipeline_id : pipeline_sequence_) {
    for (std::size_t operator_id : pipelines_[pipeline_id]->getOperatorIDs()) {
      operator_to_pipeline_lookup_[operator_id] = pipeline_id;
    }
  }
}

}  // namespace quickstep
