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

#include "transaction/ConcurrencyControl.hpp"
#include "transaction/Transaction.hpp"
#include "utility/Macros.hpp"
#include "utility/ThreadSafeQueue.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace transaction {

/**
 * @brief An admission control mechanism that grabs an exclusive lock on the
 *        entire database for any query.
 */
class DatabaseLockConcurrencyControl : public ConcurrencyControl {
 public:
  DatabaseLockConcurrencyControl() : ConcurrencyControl() {
  }

  ~DatabaseLockConcurrencyControl() override {
  }

  bool admitTransaction(
      const transaction_id tid,
      const std::vector<std::pair<ResourceId, AccessMode>> &resource_requests)
      override {
    if (waiting_transactions_.empty()) {
      // Don't bother about the requested resource IDs and their access modes,
      // as we always lock the CatalogDatabase.
      // The system currently supports only one CatalogDatabase.
      running_transaction_id_ = tid;
      return true;
    } else {
      waiting_transactions_.push(tid);
      // Other transactions are waitlisted ahead of the current transaction.
      return false;
    }
  }

  bool admitWaitingTransaction(const transaction_id tid) override {
    // The input tid is irrelevant here, because of this class' design.
    return admitNextWaitingTransaction();
  }

  void signalTransactionCompletion(const transaction_id tid) override {
    DCHECK_EQ(running_transaction_id_, tid);
    running_transaction_id_ = kInvalidTransactionID;
  }

  std::size_t getRunningTransactionsCount() const override {
    return (running_transaction_id_ == kInvalidTransactionID) ? 0u : 1u;
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
    DCHECK_EQ(kInvalidTransactionID, running_transaction_id_);
    if (!waiting_transactions_.empty()) {
      running_transaction_id_ = waiting_transactions_.popOne();
      return true;
    } else {
      LOG(WARNING) << "Trying to admit a waiting transaction when there is none";
      return false;
    }
  }

 private:
  ThreadSafeQueue<transaction_id> waiting_transactions_;

  transaction_id running_transaction_id_;

  DISALLOW_COPY_AND_ASSIGN(DatabaseLockConcurrencyControl);
};

}  // namespace transaction
}  // namespace quickstep

#endif  // QUICKSTEP_TRANSACTION_DATABASE_LOCK_CONCURRENCY_CONTROL_HPP_
