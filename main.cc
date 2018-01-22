
#include <iostream>

#include "client.h"
#include "run_test.h"

using namespace std;

extern size_t thread_cnt;
extern size_t table_limit;
extern size_t row_cnt;

void usage() {
  printf("usage: $main [thread_cnt] [table_cnt] [row_cnt]\n");
  printf("\tthread_cnt [1, 100], default 1\n");
  printf("\table_cnt [1, 1000], default 100\n");
  printf("\trow_cnt [100, 10M], default 1000\n");
  exit(-1);
}

bool parse_args(int argc, char* argv[]) {
  if (argc > 4) {
    usage();
    return false;
  }
  for (int i = 1; i < argc; i++) {
    if (i == 1) {
      thread_cnt = stol(argv[1]);
      if (thread_cnt == 0 || thread_cnt > 100) {
        usage();
        return false;
      }
    }
    if (i == 2) {
      table_limit = stol(argv[2]);
      if (table_limit == 0 || table_limit > 1000) {
        usage();
        return false;
      }
    }
    if (i == 3) {
      row_cnt = stol(argv[3]);
      if (row_cnt == 0 || row_cnt < 100 || row_cnt > 10 * 1000 * 1000) {
        usage();
        return false;
      }
    }
  }
  return true;
}

int main(int argc, char* argv[]) {
  printf("start time %lld\n", time(0));
  printf("start connecting...\n");
  Mysql client;
  if (!client.connect()) {
    printf("conn failed\n");
    return -1;
  }
  printf("conn succ\n");
  if (!parse_args(argc, argv))
    return -1;
  Init();
  StartStress();
  return 0;
}
