//
//  timer.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_TIMER_H_
#define YCSB_C_TIMER_H_


#include <time.h>
#include <stdint.h>
#include <stdio.h>
#include <chrono>

namespace utils {


// portable_timer.h
#include <time.h>
#include <sys/time.h>
#include <sys/times.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>

class PortableTimer {
public:
  void Start() { started_ = true; t0_ = now(); }
  double End()  const {
    if (!started_) return 0.0;
    const Stamp t1 = now();
    // 优先用秒+纳秒，退化到其他制
    if (t0_.kind == Stamp::K::POSIX) {
      return (t1.s - t0_.s) + (t1.ns - t0_.ns) / 1e9;
    } else if (t0_.kind == Stamp::K::GTOD) {
      return (t1.s - t0_.s) + (t1.us - t0_.us) / 1e6;
    } else { // TICKS
      long hz = sysconf(_SC_CLK_TCK);
      return (hz > 0) ? double(t1.ticks - t0_.ticks) / double(hz) : 0.0;
    }
  }

  // 自检：尝试 sleep 200ms，返回测得秒数并打印告警
  static double Sanity200ms() {
    PortableTimer t; t.Start();
    timespec req{0, 200*1000*1000};
    if (nanosleep(&req, nullptr) != 0) {
      // 某些环境 nanosleep 也不前进——忽略错误，纯测量
    }
    double s = t.End();
    fprintf(stderr, "[sanity] 200ms sleep measured = %.6f s\n", s);
    if (s == 0.0) {
      fprintf(stderr, "[warn] time source not advancing; falling back to weaker clocks\n");
    }
    return s;
  }

private:
  struct Stamp {
    enum class K { POSIX, GTOD, TICKS } kind;
    // POSIX
    int64_t s=0; int64_t ns=0;
    // GTOD
    int64_t us=0;
    // TICKS
    clock_t ticks=0;
  };

  static Stamp now() {
    Stamp st{};
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
      st.kind = Stamp::K::POSIX; st.s = ts.tv_sec; st.ns = ts.tv_nsec; return st;
    }
    // fallback: REALTIME
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
      st.kind = Stamp::K::POSIX; st.s = ts.tv_sec; st.ns = ts.tv_nsec; return st;
    }
    // fallback: gettimeofday
    timeval tv{};
    if (gettimeofday(&tv, nullptr) == 0) {
      st.kind = Stamp::K::GTOD; st.s = tv.tv_sec; st.us = tv.tv_usec; return st;
    }
    // last resort: times()
    tms buf{};
    st.kind = Stamp::K::TICKS; st.ticks = times(&buf); return st;
  }

  bool started_ = false;
  Stamp t0_{};
};



// 单调时钟计时器（纳秒→秒）
class MonotonicTimer {
public:
  void Start() { clock_gettime(CLOCK_MONOTONIC, &t0_); }
  double End() const {
    timespec t1{};
    clock_gettime(CLOCK_MONOTONIC, &t1);
    unsigned long long ns0 = (unsigned long long)t0_.tv_sec * 1000000000ull + (unsigned long long)t0_.tv_nsec;
    unsigned long long ns1 = (unsigned long long)t1.tv_sec * 1000000000ull + (unsigned long long)t1.tv_nsec;
    return (ns1 - ns0) / 1e9; // 秒
  }
private:
  timespec t0_{};
};

// 快速 200ms 自检：应当输出 ~0.200000 s
static inline double now_monotonic_s() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec / 1e9;
}
static inline void sanity_timer_200ms() {
  double t0 = now_monotonic_s();
  timespec req{0, 200*1000*1000}; // 200ms
  nanosleep(&req, nullptr);
  double t1 = now_monotonic_s();
  fprintf(stderr, "[sanity] 200ms sleep measured = %.6f s\n", t1 - t0);
}

// template <typename T>
class Timer {
 public:
  void Start() {
    time_ = Clock::now();
  }

  double End() {
    // Duration span;
    // Clock::time_point t = Clock::now();
    return std::chrono::duration<double>(Clock::now() - t0_).count();
  }

 private:
  typedef std::chrono::steady_clock Clock;
  // typedef std::chrono::duration<T> Duration;
  Clock::time_point t0_;
  Clock::time_point time_;
};

} // utils

#endif // YCSB_C_TIMER_H_

