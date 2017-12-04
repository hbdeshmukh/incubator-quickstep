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

#ifndef QUICKSTEP_TRANSACTION_CONCURRENCY_CONTROL_HPP_
#define QUICKSTEP_TRANSACTION_CONCURRENCY_CONTROL_HPP_

#include <cstddef>
#include <utility>
#include <vector>

#include "transaction/AccessMode.hpp"
#include "transaction/ResourceId.hpp"
#include "transaction/Transaction.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class ParseStatement;
class QueryHandle;

namespace transaction {

/**
 * @brief The base class for all the concurrency control mechanisms.
 */
class ConcurrencyControl {
 public:
  /**
   * @brief Constructor
   */
  ConcurrencyControl() {}

  virtual ~ConcurrencyControl() {}

  /**
   * @brief Admit a transaction to the system.
   *
   * @note Check the transaction's compatibility with the other running
   *       transactions. If it is compatible, let it run, otherwise put
   *       the transaction in the waiting list.
   *
   * @param resource_requests A vector of pairs such that each pair has a
   *        resource ID and its requested access mode.
   * @param query_handle The QueryHandle for the transaction.
   *
   * @return True if the transaction can be admitted, false if it has to wait.
   */
  virtual bool admitTransaction(
      const std::vector<std::pair<ResourceId, AccessMode>> &resource_requests,
      QueryHandle *query_handle) = 0;

  /**
   * @brief Attempt to admit a waiting transaction.
   *
   * @note Check the transaction's compatibility with the other running
   *       transactions. If it is compatible, let it run, otherwise put
   *       the transaction in the waiting list.
   *
   * @param tid The ID of the given transaction.
   *
   * @return True if the transaction can be admitted, false if the
   *         transaction has to wait.
   */
  virtual bool admitWaitingTransaction(const transaction_id tid) = 0;

  /**
   * @brief Get the next transaction for execution.
   * @return The QueryHandle to the next transaction, or nullptr if there's
   *         none.
   */
  virtual QueryHandle* getNextTransactionForExecution() = 0;

  /**
   * @brief Signal the end of a running transaction.
   *
   * @param tid The ID of the given transaction.
   **/
  virtual void signalTransactionCompletion(const transaction_id tid) = 0;

  /**
   * @brief Get the number of running transactions.
   */
  virtual std::size_t getRunningTransactionsCount() const = 0;

  /**
   * @brief Get the number of waiting transactions.
   */
  virtual std::size_t getWaitingTransactionsCount() const = 0;

  /**
   * @brief Get the access modes for each relation accessed in the query.
   *
   * @param statement The query statement.
   * @param relations The list of base relations accessed in this query.
   * @return A list of pairs of relation ID and its requested access mode.
   */
  virtual std::vector<std::pair<ResourceId, AccessMode>> getAccessModesForRelations(
      const ParseStatement &statement, const std::vector<const CatalogRelation*> relations) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(ConcurrencyControl);
};

}  // namespace transaction
}  // namespace quickstep

#endif  // QUICKSTEP_TRANSACTION_CONCURRENCY_CONTROL_HPP_
