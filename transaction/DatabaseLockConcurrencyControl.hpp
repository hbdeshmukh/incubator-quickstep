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

#ifndef QUICKSTEP_TRANSACTION_DATABASE_LOCK_CONCURRENCY_CONTROL_HPP_
#define QUICKSTEP_TRANSACTION_DATABASE_LOCK_CONCURRENCY_CONTROL_HPP_

#include <queue>
#include <utility>
#include <vector>

#include "parser/ParseStatement.hpp"
#include "query_optimizer/QueryHandle.hpp"
#include "transaction/ConcurrencyControl.hpp"
#include "transaction/Transaction.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace transaction {

/**
 * @brief An admission control mechanism that grabs an exclusive lock on the
 *        entire database for any query.
 */
class DatabaseLockConcurrencyControl : public ConcurrencyControl {
 public:
  DatabaseLockConcurrencyControl()
      : ConcurrencyControl(), running_query_handle_(nullptr) {
  }

  ~DatabaseLockConcurrencyControl() override {
  }

  /**
   * TODO(quickstep-team) When we support multiple databases, refactor this
   * method so that the ResourceId is the Database ID.
   * Right now the ResourceId is not being used.
   */
  bool admitTransaction(
      const std::vector<std::pair<ResourceId, AccessMode>> &resource_requests,
      QueryHandle *query_handle)
      override {
    if (waiting_transactions_.empty()) {
      running_query_handle_ = query_handle;
      return true;
    } else {
      waiting_transactions_.push(query_handle);
      // Other transactions are waitlisted ahead of the current transaction.
      return false;
    }
  }

  bool admitWaitingTransaction(const transaction_id tid) override {
    // The input tid is irrelevant here, because of this class' design.
    return admitNextWaitingTransaction();
  }

  void signalTransactionCompletion(const transaction_id tid) override {
    DCHECK(isTransactionRunning());
    DCHECK_EQ(running_query_handle_->query_id(), tid);
    running_query_handle_ = nullptr;
  }

  std::size_t getRunningTransactionsCount() const override {
    return isTransactionRunning() ? 1u : 0u;
  }

  std::size_t getWaitingTransactionsCount() const override {
    return waiting_transactions_.size();
  }

  /**
   * @brief Admit the next waiting transaction.
   *
   * @return True if the next waiting transaction is admitted successfully.
   *         False if there is no waiting transaction.
   */
  bool admitNextWaitingTransaction() {
    DCHECK(!isTransactionRunning());
    if (!waiting_transactions_.empty()) {
      running_query_handle_ = waiting_transactions_.front();
      waiting_transactions_.pop();
      return true;
    } else {
      LOG(WARNING) << "Trying to admit a waiting transaction when there is none";
      return false;
    }
  }

  QueryHandle* getNextTransactionForExecution() override {
    CHECK(admitNextWaitingTransaction());
    return running_query_handle_;
  }

  /**
   * TODO(quickstep-team) Handle the case when a query requires different
   * relations to have different, finer grained access modes.
   * e.g. INESRT INTO X SELECT * FROM Y;
   * One way to do that could be to pass the type of concurrency control as a
   * parameter to this query.
   */
  std::vector<std::pair<ResourceId, AccessMode>> getAccessModesForRelations(
      const ParseStatement &statement, std::vector<const CatalogRelation *> relations) override {
    transaction::AccessMode access_mode(transaction::AccessMode::NoLockMode());
    switch (statement.getStatementType()) {
      case ParseStatement::kCopy:
      case ParseStatement::kCreateIndex:
      case ParseStatement::kCreateTable:
      case ParseStatement::kDelete:
      case ParseStatement::kDropTable:
      case ParseStatement::kInsert:
      case ParseStatement::kUpdate: {
        access_mode = transaction::AccessMode::XLockMode();
        break;
      }
      case ParseStatement::kQuit:
      case ParseStatement::kSetOperation:
      case ParseStatement::kCommand: {
        break;
      }
    }

    std::vector<std::pair<ResourceId, AccessMode>> result;
    result.reserve(relations.size());
    for (const CatalogRelation *relation : relations) {
      result.emplace_back(relation->getID(), access_mode);
    }
    return result;
  }

 private:
  bool isTransactionRunning() const {
    return running_query_handle_ != nullptr;
  }

  transaction_id getRunningTransactionID() const {
    DCHECK(running_query_handle_ != nullptr);
    return running_query_handle_->query_id();
  }

  std::queue<QueryHandle*> waiting_transactions_;

  QueryHandle* running_query_handle_;

  DISALLOW_COPY_AND_ASSIGN(DatabaseLockConcurrencyControl);
};

}  // namespace transaction
}  // namespace quickstep

#endif  // QUICKSTEP_TRANSACTION_DATABASE_LOCK_CONCURRENCY_CONTROL_HPP_
