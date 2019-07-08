//  Copyright (c) 2017-present The slash Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SLASH_MUTEX_IMPL_H_
#define SLASH_MUTEX_IMPL_H_

#include "slash/include/mutex.h"

#include <memory>

namespace slash {

// Default implementation of MutexFactory.
class MutexFactoryImpl : public MutexFactory {
 public:
  std::shared_ptr<ScopeRecMutex> AllocateMutex() override;
  std::shared_ptr<ScopeRecCondVar> AllocateCondVar() override;
};

}  //  namespace slash
#endif  // SLASH_MUTEX_IMPL_H_
