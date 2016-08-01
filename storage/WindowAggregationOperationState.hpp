/**
 *   Copyright 2011-2015 Quickstep Technologies LLC.
 *   Copyright 2015-2016 Pivotal Software, Inc.
 *   Copyright 2016, Quickstep Research Group, Computer Sciences Department,
 *     University of Wisconsin—Madison.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 **/

#ifndef QUICKSTEP_STORAGE_WINDOW_AGGREGATION_OPERATION_STATE_HPP_
#define QUICKSTEP_STORAGE_WINDOW_AGGREGATION_OPERATION_STATE_HPP_

#include <cstddef>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/scalar/Scalar.hpp"
#include "expressions/scalar/ScalarAttribute.hpp"
#include "expressions/window_aggregation/WindowAggregationHandle.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/WindowAggregationOperationState.pb.h"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogDatabaseLite;
class CatalogRelationSchema;
class InsertDestination;
class StorageManager;
class WindowAggregateFunction;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief Helper class for maintaining the state of window aggregation. 
 **/
class WindowAggregationOperationState {
 public:
  /**
   * @brief Constructor for window aggregation operation state.
   *
   * @param input_relation Input relation on which window aggregation is computed.
   * @param window_aggregate_functions The window aggregate function to be
   *                                   computed.
   * @param arguments A list of argument expressions to that aggregate.
   * @param partition_by_attributes A list of window partition key.
   * @param order_by_attributes A list of window order key.
   * @param is_row True if the window frame is calculated by ROW, false if it is
   *               calculated by RANGE.
   * @param num_preceding The number of rows/range for the tuples preceding the
   *                      current row. -1 means UNBOUNDED PRECEDING.
   * @param num_following The number of rows/range for the tuples following the
   *                      current row. -1 means UNBOUNDED FOLLOWING.
   * @param storage_manager The StorageManager to get block references.
   */
  WindowAggregationOperationState(const CatalogRelationSchema &input_relation,
                                  const WindowAggregateFunction *window_aggregate_function,
                                  std::vector<std::unique_ptr<const Scalar>> &&arguments,
                                  const std::vector<std::unique_ptr<const Scalar>> &partition_by_attributes,
                                  const std::vector<std::unique_ptr<const Scalar>> &order_by_attributes,
                                  const bool is_row,
                                  const std::int64_t num_preceding,
                                  const std::int64_t num_following,
                                  StorageManager *storage_manager);

  ~WindowAggregationOperationState() {}

  /**
   * @brief Generate the window aggregation operation state from the serialized
   *        Protocol Buffer representation.
   *
   * @param proto A serialized protocol buffer representation of a
   *        WindowAggregationOperationState, originally generated by the
   *        optimizer.
   * @param database The database for resolving relation and attribute
   *        references.
   * @param storage_manager The StorageManager to use.
   **/
  static WindowAggregationOperationState* ReconstructFromProto(
      const serialization::WindowAggregationOperationState &proto,
      const CatalogDatabaseLite &database,
      StorageManager *storage_manager);

  /**
   * @brief Check whether a serialization::AggregationOperationState is
   *        fully-formed and all parts are valid.
   *
   * @param proto A serialized Protocol Buffer representation of an
   *        AggregationOperationState, originally generated by the optimizer.
   * @param database The Database to resolve relation and attribute references
   *        in.
   * @return Whether proto is fully-formed and valid.
   **/
  static bool ProtoIsValid(const serialization::WindowAggregationOperationState &proto,
                           const CatalogDatabaseLite &database);

  /**
   * @brief Compute window aggregates on the tuples of the given relation.
   *
   * @param output_destination The output destination for the computed window
   *                           aggregate.
   * @param block_ids The id of the blocks to be computed.
   **/
  void windowAggregateBlocks(InsertDestination *output_destination,
                             const std::vector<block_id> &block_ids);

 private:
  const CatalogRelationSchema &input_relation_;
  const std::vector<block_id> block_ids_;
  std::unique_ptr<WindowAggregationHandle> window_aggregation_handle_;
  std::vector<std::unique_ptr<const Scalar>> arguments_;
  StorageManager *storage_manager_;

  DISALLOW_COPY_AND_ASSIGN(WindowAggregationOperationState);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_WINDOW_AGGREGATION_OPERATION_STATE_HPP_