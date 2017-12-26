
#include <time.h>

#include <functional>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <boost/algorithm/string.hpp>
#include "client.h"
#include "run_test.h"

using namespace std;

//typedef function<void()> executor;
//vector<executor> executors;
vector<string> contents;

struct TableInfo {
  string name;
  recursive_mutex lock;
  TableInfo(const std::string& str) : name(str) {}
};
map<string, TableInfo*> lock_table;
recursive_mutex g_lock;

size_t thread_cnt;

enum op_type_t {
  kCreateTable = 0,
  kDropTable,
  kAlterTable,
  kInsert,
  kDelete,
  kUpdate,
  kQuery
};

enum field_t {
  kOrderKey = 0,
  kPartKey,
  kSuppKey,
  kLineNumber,
  kQuantity = 4,
  kExtendedPrice,
  kDiscount,
  kTax,
  kReturnFlag,
  kLineStatus = 9,
  kShipDate,
  kCommitDate,
  kRecepitDate,
  kShipinStruct,
  kShipMode = 14,
  kComment
};

const int kTableLimit = 10;
const int kLineLimit = 1000;
string TablePrefix = "terark_";

void Init() {
  srand (time(NULL));
  string path = "./lineitem_2m.tbl";
  ifstream in(path.c_str());
  string line;
  while (getline(in, line)) {
    if (line.size() < 10)
      continue;
    contents.push_back(line);
  }
}

void execute(int tid) {
  while (true) {
    int type = rand() % 7;
    switch (type) {
    case kCreateTable:
      CreateTable(-1);
      break;
    case kDropTable:
      DropTable();
      break;
    case kInsert:
      Insert();
      break;
    case kQuery:
      //Query();
      break;
    }
  }
}

void StartStress() {
  std::vector<std::thread> threads;
  // Launch a group of threads
  for (size_t i = 0; i < thread_cnt; ++i) {
    threads.push_back(std::thread(execute, i));
  }
  // Join the threads with the main thread
  for (size_t i = 0; i < thread_cnt; ++i) {
    threads[i].join();
  }
  threads.clear();
}


/*
 * only for test, no thread id kept
 */
bool try_lock(const std::string& table) {
  std::lock_guard<std::recursive_mutex> lock(g_lock);
  if (lock_table.count(table) == 0) {
    TableInfo* tinfo = new TableInfo(table);
    tinfo->lock.lock();
    lock_table.insert(make_pair(table, tinfo));
    return true;
  } else if (lock_table[table]->lock.try_lock()) {
    return true;
  }
  return false;
}

void release_lock(const std::string& table) {
  std::lock_guard<std::recursive_mutex> lock(g_lock);
  if (lock_table.count(table) == 0)
    return;
  else
    lock_table[table]->lock.unlock();
}

void CreateTable(int idx) {
  if (idx == -1)
    idx = rand() % kTableLimit;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;

  printf("CreateTable: terark_%d\n", idx);
  stringstream ss;
  ss << "(id BIGINT NOT NULL AUTO_INCREMENT, "
     << "L_ORDERKEY    INT NOT NULL, "
     << "L_PARTKEY     INT NOT NULL,"
     << "L_SUPPKEY     INTEGER NOT NULL,"
     << "L_LINENUMBER  INTEGER NOT NULL,"
     << "L_QUANTITY    DECIMAL(15,2) NOT NULL,"
     << "L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,"
     << "L_DISCOUNT    DECIMAL(15,2) NOT NULL,"
     << "L_TAX         DECIMAL(15,2) NOT NULL,"
     << "L_RETURNFLAG  CHAR(1) NOT NULL,"
     << "L_LINESTATUS  CHAR(1) NOT NULL,"
    //<< "L_SHIPDATE    DATE NOT NULL,"
    //<< "L_COMMITDATE  DATE NOT NULL,"
    //<< "L_RECEIPTDATE DATE NOT NULL,"
     << "L_SHIPINSTRUCT CHAR(25) NOT NULL,"
     << "L_SHIPMODE     CHAR(10) NOT NULL,"
     << "L_COMMENT      VARCHAR(512) NOT NULL,"
     << "PRIMARY KEY (id),"
     << "KEY (L_ORDERKEY, L_PARTKEY),"
     << "KEY (L_ORDERKEY),"
     << "KEY (L_ORDERKEY, L_SUPPKEY),"
     << "KEY (L_PARTKEY),"
     << "KEY (L_PARTKEY, L_ORDERKEY),"
     << "KEY (L_PARTKEY, L_SUPPKEY),"
     << "KEY (L_SUPPKEY),"
     << "KEY (L_SUPPKEY, L_ORDERKEY),"
     << "KEY (L_SUPPKEY, L_PARTKEY));";
  string stmt = "create table if not exists " + table + ss.str();
  Mysql client;
  if (!client.connect()) {
    printf("CreateTable(): conn failed\n");
    return;
  }
  MYSQL_STMT* m_stmt = client.prepare(stmt);
  client.execute(m_stmt);
  release_lock(table);
  printf("done CreateTable: terark_%d\n", idx);
}

void DropTable() {
  int idx = rand() % kTableLimit;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  printf("Drop table: terark_%d\n", idx);
  string stmt = "drop table if exists " + table;
  Mysql client;
  if (!client.connect()) {
    printf("DropTable(): conn failed\n");
    return;
  }
  MYSQL_STMT* m_stmt = client.prepare(stmt);
  client.execute(m_stmt);
  release_lock(table);
  printf("done Drop table: terark_%d\n", idx);
}

/*
 * TBD:
 * 1. add back date fields
 * 2. to employ txn + bulk-insert
 */
void Insert() {
  Mysql client;
  if (!client.connect()) {
    printf("Insert(): conn failed\n");
    return;
  }
  int idx = rand() % kTableLimit;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(idx);
  
  string str_stmt = "Insert into " + table +
    " values(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
  MYSQL_STMT* stmt = client.prepare(str_stmt);
  int row_start = rand() % contents.size();
  int limit = min<size_t>(kLineLimit, contents.size() - row_start + 1);
  printf("Insert table: terark_%d, cnt %d\n", idx, limit);
  for (int cnt = 0; cnt < kLineLimit; cnt++) {
    if (row_start + cnt >= contents.size())
      break;
    MYSQL_BIND in_params[17];
    memset(in_params, 0, sizeof(in_params));
    string& line = contents[cnt + row_start];
    std::vector<std::string> results;
    boost::split(results, line, [](char c){return c == '|';});

    client.bind_arg(in_params[0], stoi(results[kOrderKey]));
    client.bind_arg(in_params[1], stoi(results[kPartKey]));

    client.bind_arg(in_params[2], stoi(results[kSuppKey]));
    client.bind_arg(in_params[3], stoi(results[kLineNumber]));

    client.bind_arg(in_params[4], stod(results[kQuantity]));
    client.bind_arg(in_params[5], stod(results[kExtendedPrice]));
    client.bind_arg(in_params[6], stod(results[kDiscount]));
    client.bind_arg(in_params[7], stod(results[kTax]));

    client.bind_arg(in_params[8], results[kReturnFlag].c_str(), 1);
    client.bind_arg(in_params[9], results[kLineStatus].c_str(), 1);

    // shipdate = 10
    // commitdate = 11
    // receipt = 12

    client.bind_arg(in_params[13], results[kShipinStruct].c_str(), 25);
    client.bind_arg(in_params[14], results[kShipMode].c_str(), 10);
    client.bind_arg(in_params[15], results[kComment].c_str(), 512);

    client.bind_execute(stmt, in_params);
  }
  client.release_stmt(stmt);
  release_lock(table);
  printf("done Insert table: terark_%d\n", idx);
}

void Delete() {
}

// query with primary key? or secondary key
/*void Query() {
  Mysql client;
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  int idx = rand() % kTableLimit;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  string str_stmt = "Insert into " + table +
    " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
  MYSQL_STMT* stmt = client.prepare(str_stmt);
  int row_start = rand() % contents.size();
  for (int cnt = 0; cnt < kLineLimit; cnt++) {
    if (row_start + cnt >= contents.size())
      break;
    MYSQL_BIND in_params[17];
    memset(in_params, 0, sizeof(in_params));
    string& line = contents[cnt + row_start];
    std::vector<std::string> results;
    boost::split(results, line, [](char c){return c == '|';});

    client.bind_arg(in_params[0], stoi(results[kOrderKey]));
    client.bind_arg(in_params[1], stoi(results[kPartKey]));

    client.bind_arg(in_params[2], stoi(results[kSuppKey]));
    client.bind_arg(in_params[3], stoi(results[kLineNumber]));

    client.bind_arg(in_params[4], stod(results[kQuantity]));
    client.bind_arg(in_params[5], stod(results[kExtendedPrice]));
    client.bind_arg(in_params[6], stod(results[kDiscount]));
    client.bind_arg(in_params[7], stod(results[kTax]));

    client.bind_arg(in_params[8], results[kReturnFlag].c_str(), 1);
    client.bind_arg(in_params[9], results[kLineStatus].c_str(), 1);

    // shipdate = 10
    // commitdate = 11
    // receipt = 12

    client.bind_arg(in_params[13], results[kShipinStruct].c_str(), 25);
    client.bind_arg(in_params[14], results[kShipMode].c_str(), 10);
    client.bind_arg(in_params[15], results[kComment].c_str(), 512);

    client.bind_execute(stmt, in_params);
  }
  client.release_stmt(stmt);
  release_lock(table);
}
*/

