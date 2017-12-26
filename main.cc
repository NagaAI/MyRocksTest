
#include <iostream>

#include "client.h"
#include "run_test.h"

using namespace std;

extern size_t thread_cnt;
int main(int argc, char* argv[]) {
  printf("start connecting...\n");
  Mysql client;
  if (!client.connect()) {
    printf("conn failed\n");
    return -1;
  }
  printf("conn succ\n");
  thread_cnt = 1;
  if (argc > 2) {
    printf("usage:  $ main [thread_cnt]\n");
    return -1;
  } else if (argc == 2) {
    thread_cnt = stol(argv[1]);
    if (thread_cnt > 50) {
      printf("thread cnt toooo large\n");
      return -1;
    }
  }
  Init();
  StartStress();
  return 0;
}
