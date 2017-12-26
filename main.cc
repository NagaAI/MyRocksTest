
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
  printf("conn succ\n");
  //for (int i = 0; i < 5; i++)
  //CreateTable();
  Init();
  printf("start insert...\n");
  Insert();
  printf("insert done\n");
  
  return 0;
}
