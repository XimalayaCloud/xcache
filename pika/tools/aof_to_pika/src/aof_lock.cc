#include <pthread.h>
#include <cstdlib>
#include <string.h>
#include <iostream>
#include <sys/time.h>

#include "aof_lock.h"


static void PthreadCall(const std::string &label, int result) {
	if (result != 0) {
		std::cout << "pthread " << label << " : " <<  strerror(result) << std::endl;
		abort();
	}
}

// Return false if timeout
static bool PthreadTimeoutCall(const char* label, int result) {
  if (result != 0) {
    if (result == ETIMEDOUT) {
      return false;
    }
    std::cout << "pthread " << label << " : " << strerror(result) << std::endl;
    abort();
  }
  return true;
}

Mutex::Mutex() { PthreadCall("init mutex", pthread_mutex_init(&mu_, NULL)); }

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); }

void Mutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }

CondVar::CondVar(Mutex* mu)
	: mu_(mu) {
		pthread_condattr_t condattr;
		PthreadCall("pthread_condattr_init", pthread_condattr_init(&condattr));
		PthreadCall("pthread_condattr_setclock", pthread_condattr_setclock(&condattr, CLOCK_MONOTONIC));
		PthreadCall("init cv", pthread_cond_init(&cv_, &condattr));
		PthreadCall("pthread_condattr_destroy", pthread_condattr_destroy(&condattr));
		//PthreadCall("init cv", pthread_cond_init(&cv_, NULL));
	}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() {
	PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
}

// return false if timeout ms
bool CondVar::TimedWait(uint32_t timeout) {
  /*
   * pthread_cond_timedwait api use absolute API
   * so we need clock_gettime + timeout
   */
  struct timespec tsp;
  clock_gettime(CLOCK_MONOTONIC, &tsp);

  int64_t nsec = tsp.tv_nsec + timeout * 1000000LL;
  tsp.tv_sec = tsp.tv_sec + nsec / 1000000000;
  tsp.tv_nsec = nsec % 1000000000;

  return PthreadTimeoutCall("timewait",
	  pthread_cond_timedwait(&cv_, &mu_->mu_, &tsp));
}

void CondVar::Signal() {
	PthreadCall("signal", pthread_cond_signal(&cv_));
}

void CondVar::SignalAll() {
	PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
}
