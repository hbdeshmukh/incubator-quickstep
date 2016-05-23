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

#ifndef QUICKSTEP_RELATIONAL_OPERATORS_HASH_JOIN_OPERATOR_HPP_
#define QUICKSTEP_RELATIONAL_OPERATORS_HASH_JOIN_OPERATOR_HPP_

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"

#ifdef QUICKSTEP_HAVE_LIBNUMA
#include "catalog/NUMAPlacementScheme.hpp"
#endif

#include "catalog/PartitionSchemeHeader.hpp"
#include "query_execution/QueryContext.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/WorkOrder.hpp"
#include "storage/HashTable.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

#include "tmb/id_typedefs.h"

namespace tmb { class MessageBus; }

namespace quickstep {

class CatalogRelationSchema;
class InsertDestination;
class Predicate;
class Scalar;
class StorageManager;
class WorkOrdersContainer;

/** \addtogroup RelationalOperators
 *  @{
 */

/**
 * @brief An operator which performs a hash-join, including inner-join,
 *        semi-join, anti-join and outer-join on two relations.
 **/
class HashJoinOperator : public RelationalOperator {
 public:
  enum class JoinType {
    kInnerJoin = 0,
    kLeftSemiJoin,
    kLeftAntiJoin,
    kLeftOuterJoin
  };

  /**
   * @brief Constructor.
   *
   * @note This operator can be constructed with an optional parameter
   *       residual_predicate_index that applies an additional filter besides
   *       the key-equality predicate. If the residual_predicate_index is not
   *       kInvalidPredicateId, then the key-equality predicate will still be
   *       evaluated using vectorized probing of the hash-table, but the
   *       residual predicate will be checked using non-vectorized
   *       tuple-at-a-time evaluation (similar to NestedLoopsJoinOperator).
   *       Alternatively, a HashJoinOperator can be used to evaluate just the
   *       key-equality predicate, and its output can be piped to a subsequent
   *       SelectOperator that can evaluate the residual predicate on the
   *       materialized output of the hash join with full vectorization support
   *       (although at the cost of making an additional intermediate data
   *       copy). Which approach performs best in practice is likely to be
   *       dependent on the selectivity of the predicates involved. The decision
   *       is left to the query optimizer.
   *
   * @param build_relation The relation that the hash table was originally
   *        built on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param probe_relation_is_stored If probe_relation is a stored relation or
   *        not. False if probe_relation is the output of some operator in the
   *        query plan DAG. True if probe_relation is a stored relation.
   * @param join_key_attributes The IDs of equijoin attributes in
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param output_relation The output relation.
   * @param output_destination_index The index of the InsertDestination in the
   *        QueryContext to insert the join results.
   * @param hash_table_index The index of the JoinHashTable in QueryContext.
   * @param residual_predicate_index If not kInvalidPredicateId, apply as an
   *        additional filter to pairs of tuples that match the hash-join (i.e.
   *        key equality) predicate. Effectively, this makes the join predicate
   *        the conjunction of the key-equality predicate and residual predicate.
   *        Note that this field does not apply to outer joins.
   * @param selection_index The group index of Scalars in QueryContext,
   *        corresponding to the attributes of the relation referred by
   *        output_relation_id. Each Scalar is evaluated for the joined tuples,
   *        and the resulting value is inserted into the join result.
   * @param is_selection_on_build Whether each selection Scalar is using attributes
   *        from the build relation as input. Can be NULL for inner/semi/anti
   *        joins since this information is not utilized by these joins.
   * @param join_type The type of join corresponding to this operator.
   **/
  HashJoinOperator(const CatalogRelation &build_relation,
                   const CatalogRelation &probe_relation,
                   const bool probe_relation_is_stored,
                   const std::vector<attribute_id> &join_key_attributes,
                   const bool any_join_key_attributes_nullable,
                   const CatalogRelation &output_relation,
                   const QueryContext::insert_destination_id output_destination_index,
                   const QueryContext::join_hash_table_group_id hash_table_group_index,
                   const QueryContext::predicate_id residual_predicate_index,
                   const QueryContext::scalar_group_id selection_index,
                   bool is_numa_aware_join,
                   const std::vector<bool> *is_selection_on_build = nullptr,
                   const JoinType join_type = JoinType::kInnerJoin)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        probe_relation_is_stored_(probe_relation_is_stored),
        join_key_attributes_(join_key_attributes),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        output_relation_(output_relation),
        output_destination_index_(output_destination_index),
        hash_table_group_index_(hash_table_group_index),
        residual_predicate_index_(residual_predicate_index),
        selection_index_(selection_index),
        is_numa_aware_join_(is_numa_aware_join),
        is_selection_on_build_(is_selection_on_build == nullptr
                                   ? std::vector<bool>()
                                   : *is_selection_on_build),
        join_type_(join_type),
        probe_relation_block_ids_(probe_relation_is_stored
                                      ? probe_relation.getBlocksSnapshot()
                                      : std::vector<block_id>()),
        num_workorders_generated_(0),
        started_(false) {
    DCHECK(join_type != JoinType::kLeftOuterJoin ||
               (is_selection_on_build != nullptr &&
                residual_predicate_index == QueryContext::kInvalidPredicateId));
#ifdef QUICKSTEP_HAVE_LIBNUMA
    probe_relation_placement_scheme_ = probe_relation.getNUMAPlacementSchemePtr();
#endif
    if (probe_relation.hasPartitionScheme()) {
      const PartitionScheme &part_scheme = probe_relation.getPartitionScheme();
      const std::size_t num_partitions = part_scheme.getPartitionSchemeHeader().getNumPartitions();
      probe_relation_block_ids_in_partition_.resize(num_partitions);
      num_workorders_generated_in_partition_.resize(num_partitions);
      num_workorders_generated_in_partition_.assign(num_partitions, 0);
      for (std::size_t part_id = 0; part_id < num_partitions; ++part_id) {
        if (probe_relation_is_stored) {
          probe_relation_block_ids_in_partition_[part_id] = part_scheme.getBlocksInPartition(part_id);
        } else {
          probe_relation_block_ids_in_partition_[part_id] = std::vector<block_id>();
        }
      }
    }
  }

  ~HashJoinOperator() override {}

  bool getAllWorkOrders(WorkOrdersContainer *container,
                        QueryContext *query_context,
                        StorageManager *storage_manager,
                        const tmb::client_id scheduler_client_id,
                        tmb::MessageBus *bus) override;

  void feedInputBlock(const block_id input_block_id,
                      const relation_id input_relation_id) override {
    DCHECK(input_relation_id == probe_relation_.getID());
    if (probe_relation_.hasPartitionScheme()) {
      const partition_id part_id =
          probe_relation_.getPartitionScheme().getPartitionForBlock(input_block_id);
      probe_relation_block_ids_in_partition_[part_id].push_back(input_block_id);
    } else {
      probe_relation_block_ids_.push_back(input_block_id);
    }
  }

  void feedInputBlocks(const relation_id rel_id,
                       std::vector<block_id> *partially_filled_blocks) override {
    DCHECK(rel_id == probe_relation_.getID());
    if (probe_relation_.hasPartitionScheme()) {
      for (auto it = partially_filled_blocks->begin(); it != partially_filled_blocks->end(); ++it) {
        const partition_id part_id = probe_relation_.getPartitionScheme().getPartitionForBlock((*it));
        probe_relation_block_ids_in_partition_[part_id].insert(probe_relation_block_ids_in_partition_[part_id].end(),
                                                               *it);
      }
    } else {
      probe_relation_block_ids_.insert(probe_relation_block_ids_.end(),
                                       partially_filled_blocks->begin(),
                                       partially_filled_blocks->end());
    }
  }

  QueryContext::insert_destination_id getInsertDestinationID() const override {
    return output_destination_index_;
  }

  const relation_id getOutputRelationID() const override {
    return output_relation_.getID();
  }

  void doneFeedingInputBlocks(const relation_id rel_id) override {
    // The HashJoinOperator depends on BuildHashOperator too, but it
    // should ignore a doneFeedingInputBlocks() message that comes
    // after completion of BuildHashOperator. Therefore we need this check.
    if (probe_relation_.getID() == rel_id) {
      done_feeding_input_relation_ = true;
    }
  }

  template <class JoinWorkOrderClass>
  void addWorkOrders(WorkOrdersContainer *container,
                     QueryContext *query_context,
                     StorageManager *storage_manager,
                     const Predicate *predicate,
                     const std::vector<std::unique_ptr<const Scalar>> &selection,
                     InsertDestination *output_destination);

  template <class JoinWorkOrderClass>
  void addWorkOrdersUsingPartitionedInput(WorkOrdersContainer *container,
                                          QueryContext *query_context,
                                          StorageManager *storage_manager,
                                          const Predicate *predicate,
                                          const std::vector<std::unique_ptr<const Scalar>> &selection,
                                          InsertDestination *output_destination);

#ifdef QUICKSTEP_HAVE_LIBNUMA
  template <class JoinWorkOrderClass>
  void addPartitionAwareWorkOrders(WorkOrdersContainer *container,
                                   QueryContext *query_context,
                                   StorageManager *storage_manager,
                                   const Predicate *predicate,
                                   const std::vector<std::unique_ptr<const Scalar>> &selection,
                                   InsertDestination *output_destination);
#endif

 private:
  template <class JoinWorkOrderClass>
  bool getAllNonOuterJoinWorkOrders(WorkOrdersContainer *container,
                                    QueryContext *query_context,
                                    StorageManager *storage_manager);

  bool getAllOuterJoinWorkOrders(WorkOrdersContainer *container,
                                 QueryContext *query_context,
                                 StorageManager *storage_manager);

  const CatalogRelation &build_relation_;
  const CatalogRelation &probe_relation_;
  const bool probe_relation_is_stored_;
  const std::vector<attribute_id> join_key_attributes_;
  const bool any_join_key_attributes_nullable_;
  const CatalogRelation &output_relation_;
  const QueryContext::insert_destination_id output_destination_index_;
  const QueryContext::join_hash_table_group_id hash_table_group_index_;
  const QueryContext::predicate_id residual_predicate_index_;
  const QueryContext::scalar_group_id selection_index_;
  bool is_numa_aware_join_;
  const std::vector<bool> is_selection_on_build_;
  const JoinType join_type_;

  std::vector<block_id> probe_relation_block_ids_;
  // A vector of vectors V where V[i] indicates the list of block IDs of the
  // input relation that belong to the partition i.
  std::vector<std::vector<block_id>> probe_relation_block_ids_in_partition_;
  std::size_t num_workorders_generated_;
  std::vector<std::size_t> num_workorders_generated_in_partition_;

  bool started_;
#ifdef QUICKSTEP_HAVE_LIBNUMA
  const NUMAPlacementScheme *probe_relation_placement_scheme_;
#endif
  DISALLOW_COPY_AND_ASSIGN(HashJoinOperator);
};

/**
 * @brief An inner join WorkOrder produced by HashJoinOperator.
 **/
class HashInnerJoinWorkOrder : public WorkOrder {
 public:
  /**
   * @brief Constructor.
   *
   * @param build_relation The relation that the hash table was originally built
   *        on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param join_key_attributes The IDs of equijoin attributes in \c
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param lookup_block_id The block id of the probe_relation.
   * @param residual_predicate If non-null, apply as an additional filter to
   *        pairs of tuples that match the hash-join (i.e. key equality)
   *        predicate. Effectively, this makes the join predicate the
   *        conjunction of the key-equality predicate and residual_predicate.
   * @param selection A list of Scalars corresponding to the relation attributes
   *        in \c output_destination. Each Scalar is evaluated for the joined
   *        tuples, and the resulting value is inserted into the join result.
   * @param hash_table The JoinHashTable to use.
   * @param output_destination The InsertDestination to insert the join results.
   * @param storage_manager The StorageManager to use.
   **/
  HashInnerJoinWorkOrder(const CatalogRelationSchema &build_relation,
                         const CatalogRelationSchema &probe_relation,
                         const std::vector<attribute_id> &join_key_attributes,
                         const bool any_join_key_attributes_nullable,
                         const block_id lookup_block_id,
                         const Predicate *residual_predicate,
                         const std::vector<std::unique_ptr<const Scalar>> &selection,
                         const JoinHashTable &hash_table,
                         InsertDestination *output_destination,
                         StorageManager *storage_manager,
                         const numa_node_id numa_node = -1)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        join_key_attributes_(join_key_attributes),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        block_id_(lookup_block_id),
        residual_predicate_(residual_predicate),
        selection_(selection),
        hash_table_(hash_table),
        output_destination_(DCHECK_NOTNULL(output_destination)),
        storage_manager_(DCHECK_NOTNULL(storage_manager)) {
    if (numa_node != -1) {
      preferred_numa_nodes_.push_back(numa_node);
    }
  }

  /**
   * @brief Constructor for the distributed version.
   *
   * @param build_relation The relation that the hash table was originally built
   *        on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param join_key_attributes The IDs of equijoin attributes in \c
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param lookup_block_id The block id of the probe_relation.
   * @param residual_predicate If non-null, apply as an additional filter to
   *        pairs of tuples that match the hash-join (i.e. key equality)
   *        predicate. Effectively, this makes the join predicate the
   *        conjunction of the key-equality predicate and residual_predicate.
   * @param selection A list of Scalars corresponding to the relation attributes
   *        in \c output_destination. Each Scalar is evaluated for the joined
   *        tuples, and the resulting value is inserted into the join result.
   * @param hash_table The JoinHashTable to use.
   * @param output_destination The InsertDestination to insert the join results.
   * @param storage_manager The StorageManager to use.
   **/
  HashInnerJoinWorkOrder(const CatalogRelationSchema &build_relation,
                         const CatalogRelationSchema &probe_relation,
                         std::vector<attribute_id> &&join_key_attributes,
                         const bool any_join_key_attributes_nullable,
                         const block_id lookup_block_id,
                         const Predicate *residual_predicate,
                         const std::vector<std::unique_ptr<const Scalar>> &selection,
                         const JoinHashTable &hash_table,
                         InsertDestination *output_destination,
                         StorageManager *storage_manager,
                         const numa_node_id numa_node = -1)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        join_key_attributes_(std::move(join_key_attributes)),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        block_id_(lookup_block_id),
        residual_predicate_(residual_predicate),
        selection_(selection),
        hash_table_(hash_table),
        output_destination_(DCHECK_NOTNULL(output_destination)),
        storage_manager_(DCHECK_NOTNULL(storage_manager)) {
    if (numa_node != -1) {
      preferred_numa_nodes_.push_back(numa_node);
    }
  }

  ~HashInnerJoinWorkOrder() override {}

  /**
   * @exception TupleTooLargeForBlock A tuple produced by this join was too
   *            large to insert into an empty block provided by
   *            output_destination_index_ in query_context. Join may be
   *            partially complete (with some tuples inserted into the
   *            destination) when this exception is thrown, causing potential
   *            inconsistency.
   **/
  void execute() override;

 private:
  template <typename CollectorT>
  void executeWithCollectorType();

  const CatalogRelationSchema &build_relation_;
  const CatalogRelationSchema &probe_relation_;
  const std::vector<attribute_id> join_key_attributes_;
  const bool any_join_key_attributes_nullable_;
  const block_id block_id_;
  const Predicate *residual_predicate_;
  const std::vector<std::unique_ptr<const Scalar>> &selection_;
  const JoinHashTable &hash_table_;

  InsertDestination *output_destination_;
  StorageManager *storage_manager_;

  DISALLOW_COPY_AND_ASSIGN(HashInnerJoinWorkOrder);
};

/**
 * @brief A left semi-join WorkOrder produced by the HashJoinOperator to execute
 *        EXISTS() clause.
 **/
class HashSemiJoinWorkOrder : public WorkOrder {
 public:
  /**
   * @brief Constructor.
   *
   * @param build_relation The relation that the hash table was originally built
   *        on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param join_key_attributes The IDs of equijoin attributes in \c
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param lookup_block_id The block id of the probe_relation.
   * @param residual_predicate If non-null, apply as an additional filter to
   *        pairs of tuples that match the hash-join (i.e. key equality)
   *        predicate. Effectively, this makes the join predicate the
   *        conjunction of the key-equality predicate and residual_predicate.
   * @param selection A list of Scalars corresponding to the relation attributes
   *        in \c output_destination. Each Scalar is evaluated for the joined
   *        tuples, and the resulting value is inserted into the join result.
   * @param hash_table The JoinHashTable to use.
   * @param output_destination The InsertDestination to insert the join results.
   * @param storage_manager The StorageManager to use.
   **/
  HashSemiJoinWorkOrder(const CatalogRelationSchema &build_relation,
                        const CatalogRelationSchema &probe_relation,
                        const std::vector<attribute_id> &join_key_attributes,
                        const bool any_join_key_attributes_nullable,
                        const block_id lookup_block_id,
                        const Predicate *residual_predicate,
                        const std::vector<std::unique_ptr<const Scalar>> &selection,
                        const JoinHashTable &hash_table,
                        InsertDestination *output_destination,
                        StorageManager *storage_manager,
                        const numa_node_id numa_node = -1)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        join_key_attributes_(join_key_attributes),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        block_id_(lookup_block_id),
        residual_predicate_(residual_predicate),
        selection_(selection),
        hash_table_(hash_table),
        output_destination_(DCHECK_NOTNULL(output_destination)),
        storage_manager_(DCHECK_NOTNULL(storage_manager)) {
    if (numa_node != -1) {
      preferred_numa_nodes_.push_back(numa_node);
    }
  }

  /**
   * @brief Constructor for the distributed version.
   *
   * @param build_relation The relation that the hash table was originally built
   *        on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param join_key_attributes The IDs of equijoin attributes in \c
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param lookup_block_id The block id of the probe_relation.
   * @param residual_predicate If non-null, apply as an additional filter to
   *        pairs of tuples that match the hash-join (i.e. key equality)
   *        predicate. Effectively, this makes the join predicate the
   *        conjunction of the key-equality predicate and residual_predicate.
   * @param selection A list of Scalars corresponding to the relation attributes
   *        in \c output_destination. Each Scalar is evaluated for the joined
   *        tuples, and the resulting value is inserted into the join result.
   * @param hash_table The JoinHashTable to use.
   * @param output_destination The InsertDestination to insert the join results.
   * @param storage_manager The StorageManager to use.
   **/
  HashSemiJoinWorkOrder(const CatalogRelationSchema &build_relation,
                        const CatalogRelationSchema &probe_relation,
                        std::vector<attribute_id> &&join_key_attributes,
                        const bool any_join_key_attributes_nullable,
                        const block_id lookup_block_id,
                        const Predicate *residual_predicate,
                        const std::vector<std::unique_ptr<const Scalar>> &selection,
                        const JoinHashTable &hash_table,
                        InsertDestination *output_destination,
                        StorageManager *storage_manager,
                        const numa_node_id numa_node = -1)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        join_key_attributes_(std::move(join_key_attributes)),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        block_id_(lookup_block_id),
        residual_predicate_(residual_predicate),
        selection_(selection),
        hash_table_(hash_table),
        output_destination_(DCHECK_NOTNULL(output_destination)),
        storage_manager_(DCHECK_NOTNULL(storage_manager)) {
    if (numa_node != -1) {
      preferred_numa_nodes_.push_back(numa_node);
    }
  }

  ~HashSemiJoinWorkOrder() override {}

  void execute() override;

 private:
  void executeWithoutResidualPredicate();

  void executeWithResidualPredicate();

  const CatalogRelationSchema &build_relation_;
  const CatalogRelationSchema &probe_relation_;
  const std::vector<attribute_id> join_key_attributes_;
  const bool any_join_key_attributes_nullable_;
  const block_id block_id_;
  const Predicate *residual_predicate_;
  const std::vector<std::unique_ptr<const Scalar>> &selection_;
  const JoinHashTable &hash_table_;

  InsertDestination *output_destination_;
  StorageManager *storage_manager_;

  DISALLOW_COPY_AND_ASSIGN(HashSemiJoinWorkOrder);
};

/**
 * @brief A left anti-join WorkOrder produced by the HashJoinOperator to execute
 *        NOT EXISTS() clause.
 **/
class HashAntiJoinWorkOrder : public WorkOrder {
 public:
  /**
   * @brief Constructor.
   *
   * @param build_relation The relation that the hash table was originally built
   *        on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param join_key_attributes The IDs of equijoin attributes in \c
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param lookup_block_id The block id of the probe_relation.
   * @param residual_predicate If non-null, apply as an additional filter to
   *        pairs of tuples that match the hash-join (i.e. key equality)
   *        predicate. Effectively, this makes the join predicate the
   *        conjunction of the key-equality predicate and residual_predicate.
   * @param selection A list of Scalars corresponding to the relation attributes
   *        in \c output_destination. Each Scalar is evaluated for the joined
   *        tuples, and the resulting value is inserted into the join result.
   * @param hash_table The JoinHashTable to use.
   * @param output_destination The InsertDestination to insert the join results.
   * @param storage_manager The StorageManager to use.
   **/
  HashAntiJoinWorkOrder(const CatalogRelationSchema &build_relation,
                        const CatalogRelationSchema &probe_relation,
                        const std::vector<attribute_id> &join_key_attributes,
                        const bool any_join_key_attributes_nullable,
                        const block_id lookup_block_id,
                        const Predicate *residual_predicate,
                        const std::vector<std::unique_ptr<const Scalar>> &selection,
                        const JoinHashTable &hash_table,
                        InsertDestination *output_destination,
                        StorageManager *storage_manager,
                        const numa_node_id numa_node = -1)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        join_key_attributes_(join_key_attributes),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        block_id_(lookup_block_id),
        residual_predicate_(residual_predicate),
        selection_(selection),
        hash_table_(hash_table),
        output_destination_(DCHECK_NOTNULL(output_destination)),
        storage_manager_(DCHECK_NOTNULL(storage_manager)) {
    if (numa_node != -1) {
      preferred_numa_nodes_.push_back(numa_node);
    }
  }

  /**
   * @brief Constructor for the distributed version.
   *
   * @param build_relation The relation that the hash table was originally built
   *        on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param join_key_attributes The IDs of equijoin attributes in \c
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param lookup_block_id The block id of the probe_relation.
   * @param residual_predicate If non-null, apply as an additional filter to
   *        pairs of tuples that match the hash-join (i.e. key equality)
   *        predicate. Effectively, this makes the join predicate the
   *        conjunction of the key-equality predicate and residual_predicate.
   * @param selection A list of Scalars corresponding to the relation attributes
   *        in \c output_destination. Each Scalar is evaluated for the joined
   *        tuples, and the resulting value is inserted into the join result.
   * @param hash_table The JoinHashTable to use.
   * @param output_destination The InsertDestination to insert the join results.
   * @param storage_manager The StorageManager to use.
   **/
  HashAntiJoinWorkOrder(const CatalogRelationSchema &build_relation,
                        const CatalogRelationSchema &probe_relation,
                        std::vector<attribute_id> &&join_key_attributes,
                        const bool any_join_key_attributes_nullable,
                        const block_id lookup_block_id,
                        const Predicate *residual_predicate,
                        const std::vector<std::unique_ptr<const Scalar>> &selection,
                        const JoinHashTable &hash_table,
                        InsertDestination *output_destination,
                        StorageManager *storage_manager,
                        const numa_node_id numa_node = -1)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        join_key_attributes_(std::move(join_key_attributes)),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        block_id_(lookup_block_id),
        residual_predicate_(residual_predicate),
        selection_(selection),
        hash_table_(hash_table),
        output_destination_(DCHECK_NOTNULL(output_destination)),
        storage_manager_(DCHECK_NOTNULL(storage_manager)) {
    if (numa_node != -1) {
      preferred_numa_nodes_.push_back(numa_node);
    }
  }

  ~HashAntiJoinWorkOrder() override {}

  void execute() override {
    if (residual_predicate_ == nullptr) {
      executeWithoutResidualPredicate();
    } else {
      executeWithResidualPredicate();
    }
  }

 private:
  void executeWithoutResidualPredicate();

  void executeWithResidualPredicate();

  const CatalogRelationSchema &build_relation_;
  const CatalogRelationSchema &probe_relation_;
  const std::vector<attribute_id> join_key_attributes_;
  const bool any_join_key_attributes_nullable_;
  const block_id block_id_;
  const Predicate *residual_predicate_;
  const std::vector<std::unique_ptr<const Scalar>> &selection_;
  const JoinHashTable &hash_table_;

  InsertDestination *output_destination_;
  StorageManager *storage_manager_;

  DISALLOW_COPY_AND_ASSIGN(HashAntiJoinWorkOrder);
};

/**
 * @brief A left outer join WorkOrder produced by the HashJoinOperator.
 **/
class HashOuterJoinWorkOrder : public WorkOrder {
 public:
  /**
   * @brief Constructor.
   *
   * @param build_relation The relation that the hash table was originally built
   *        on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param join_key_attributes The IDs of equijoin attributes in \c
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param hash_table The JoinHashTable to use.
   * @param selection A list of Scalars corresponding to the relation attributes
   *        in \c output_destination. Each Scalar is evaluated for the joined
   *        tuples, and the resulting value is inserted into the join result.
   * @param is_selection_on_build Whether each Scalar in the \p selection vector
   *        is using attributes from the build relation as input. Note that the
   *        length of this vector should equal the length of \p selection.
   * @param lookup_block_id The block id of the probe_relation.
   * @param output_destination The InsertDestination to insert the join results.
   * @param storage_manager The StorageManager to use.
   **/
  HashOuterJoinWorkOrder(const CatalogRelationSchema &build_relation,
                         const CatalogRelationSchema &probe_relation,
                         const std::vector<attribute_id> &join_key_attributes,
                         const bool any_join_key_attributes_nullable,
                         const block_id lookup_block_id,
                         const std::vector<std::unique_ptr<const Scalar>> &selection,
                         const std::vector<bool> &is_selection_on_build,
                         const JoinHashTable &hash_table,
                         InsertDestination *output_destination,
                         StorageManager *storage_manager)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        join_key_attributes_(join_key_attributes),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        block_id_(lookup_block_id),
        selection_(selection),
        is_selection_on_build_(is_selection_on_build),
        hash_table_(hash_table),
        output_destination_(output_destination),
        storage_manager_(storage_manager) {}

  /**
   * @brief Constructor for the distributed version.
   *
   * @param build_relation The relation that the hash table was originally built
   *        on (i.e. the inner relation in the join).
   * @param probe_relation The relation to probe the hash table with (i.e. the
   *        outer relation in the join).
   * @param join_key_attributes The IDs of equijoin attributes in \c
   *        probe_relation.
   * @param any_join_key_attributes_nullable If any attribute is nullable.
   * @param hash_table The JoinHashTable to use.
   * @param selection A list of Scalars corresponding to the relation attributes
   *        in \c output_destination. Each Scalar is evaluated for the joined
   *        tuples, and the resulting value is inserted into the join result.
   * @param is_selection_on_build Whether each Scalar in the \p selection vector
   *        is using attributes from the build relation as input. Note that the
   *        length of this vector should equal the length of \p selection.
   * @param lookup_block_id The block id of the probe_relation.
   * @param output_destination The InsertDestination to insert the join results.
   * @param storage_manager The StorageManager to use.
   **/
  HashOuterJoinWorkOrder(const CatalogRelationSchema &build_relation,
                         const CatalogRelationSchema &probe_relation,
                         std::vector<attribute_id> &&join_key_attributes,
                         const bool any_join_key_attributes_nullable,
                         const block_id lookup_block_id,
                         const std::vector<std::unique_ptr<const Scalar>> &selection,
                         std::vector<bool> &&is_selection_on_build,
                         const JoinHashTable &hash_table,
                         InsertDestination *output_destination,
                         StorageManager *storage_manager)
      : build_relation_(build_relation),
        probe_relation_(probe_relation),
        join_key_attributes_(std::move(join_key_attributes)),
        any_join_key_attributes_nullable_(any_join_key_attributes_nullable),
        block_id_(lookup_block_id),
        selection_(selection),
        is_selection_on_build_(std::move(is_selection_on_build)),
        hash_table_(hash_table),
        output_destination_(output_destination),
        storage_manager_(storage_manager) {}

  ~HashOuterJoinWorkOrder() override {}

  void execute() override;

 private:
  const CatalogRelationSchema &build_relation_;
  const CatalogRelationSchema &probe_relation_;
  const std::vector<attribute_id> join_key_attributes_;
  const bool any_join_key_attributes_nullable_;
  const block_id block_id_;
  const std::vector<std::unique_ptr<const Scalar>> &selection_;
  const std::vector<bool> is_selection_on_build_;
  const JoinHashTable &hash_table_;

  InsertDestination *output_destination_;
  StorageManager *storage_manager_;

  DISALLOW_COPY_AND_ASSIGN(HashOuterJoinWorkOrder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_RELATIONAL_OPERATORS_HASH_JOIN_OPERATOR_HPP_
