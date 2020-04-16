// from concurrency examples posted on Piazza

#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>
#include "object.h"
#include "helper.h"

// thread class
// based on given starter code in concurrency examples
class Thread : public Object {
    public:
        std::thread thread_;

        /** Starts running the thread, invoked the run() method. */
        void start() { thread_ = std::thread([this]{ this->run(); }); }

        /** Wait on this thread to terminate. */
        void join() { thread_.join(); }

        /** Subclass responsibility, the body of the run method */
        virtual void run() { check(false, "Run called on parent Thread class"); }
};

/** A convenient lock and condition variable wrapper. */
class Lock : public Object {
    public:
        std::mutex mtx_;
        std::condition_variable_any cv_;

        /** Request ownership of this lock. 
         *
         *  Note: This operation will block the current thread until the lock can 
         *  be acquired. 
         */
        void lock() { mtx_.lock(); }

        /** Release this lock (relinquish ownership). */
        void unlock() { mtx_.unlock(); }

        /** Sleep and wait for a notification on this lock. 
         *
         *  Note: After waking up, the lock is owned by the current thread and 
         *  needs released by an explicit invocation of unlock(). 
         */
        void wait() { cv_.wait(mtx_); }

        // Notify all threads waiting on this lock
        void notify_all() { cv_.notify_all(); }
};
