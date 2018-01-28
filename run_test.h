
#ifndef RUNTEST_H
#define RUNTEST_H

#include <string>

extern size_t thread_cnt;
extern size_t table_cnt;
extern size_t row_cnt;

extern void Init(const std::string&);
extern void StartStress();

#endif
