
#include <iostream>

#include "client.h"
#include "run_test.h"

using namespace std;

int main() {
  printf("start connecting...\n");
  Mysql client;
  if (!client.connect()) {
    printf("conn failed\n");
    return -1;
  }
  for (int i = 0; i < 5; i++)
    CreateTable();
  printf("conn succ\n");
  return 0;
}
