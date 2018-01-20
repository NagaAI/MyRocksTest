
#include <time.h>

#include <atomic>
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

atomic<long> total_counts = { 0 };
atomic<long> total_rounds = { 0 };
atomic<long> total_elapse = { 0 };
size_t thread_cnt = 4;
size_t table_limit = 100;
size_t insert_cnt = 1000;

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

string TablePrefix = "lineitem";

void Query(Mysql& client, MYSQL_STMT* stmt, const string& table);
void QueryExecute(Mysql& client, MYSQL_STMT* stmt, const std::string& table, int idx1, int idx2);
void QueryExecute(Mysql& client, MYSQL_STMT* stmt, int idx1, int idx2);
void AlterExecute(Mysql& client, const std::string& stmt);

void Init() {
  srand (time(NULL));
  // read in data
  string path = "./lineitem_2m.tbl";
  ifstream in(path.c_str());
  string line;
  while (getline(in, line)) {
    if (line.size() < 10)
      continue;
    contents.push_back(line);
  }
  // create database
  /*Mysql client;
  if (!client.connect()) {
    printf("CreateTable(): conn failed\n");
    exit(-1);
  }
  if (mysql_query(client, "CREATE DATABASE if not exist t_test")) {
    fprintf(stderr, "%s\n", mysql_error(con));
    mysql_close(con);
    exit(1);
  }
  */
}

void execute(int tid) {
  Mysql client;
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }

  int cycle = 0;
  while (true) {
    int idx = rand() % table_limit + 1;
    string table = TablePrefix + to_string(idx);

    string str_stmt;
    MYSQL_STMT* stmt;
    time_t start;
    if (cycle == 0) {
      str_stmt = "select * from " + table +
	" where L_ORDERKEY = ? and L_PARTKEY = ?";
      stmt = client.prepare(str_stmt);
      start = time(0);
      QueryExecute(client, stmt, kOrderKey, kPartKey);
    } else if (cycle == 1) {
      str_stmt = "select * from " + table +
	" where L_SUPPKEY = ? and L_PARTKEY = ?";
      stmt = client.prepare(str_stmt);
      start = time(0);
      QueryExecute(client, stmt, kSuppKey, kPartKey);
    } else {
      str_stmt = "select * from " + table +
	" where L_ORDERKEY = ?";
      stmt = client.prepare(str_stmt);
      start = time(0);
      QueryExecute(client, stmt, kOrderKey, -1);
    }
    cycle = (cycle + 1) % 3;

    total_counts += insert_cnt;
    total_elapse += time(0) - start;
    total_rounds++;
    if (total_rounds.load() % 20 == 0) {
      printf("== QPS %f, total query %lld, time elapse %lld\n", 
	     (double)total_counts.load() / total_elapse.load(), total_counts.load(), total_elapse.load());
    }


/*
    int type = rand() % 7;
    switch (type) {
    case kCreateTable:
      CreateTable(-1);
      break;
    case kDropTable:
      DropTable();
      break;
    case kAlterTable:
      AlterTable();
      break;
    case kInsert:
      Insert();
      break;
    case kDelete:
      Delete();
      break;
    case kQuery:
      Query();
      break;
    }
*/
  }
}

void StartStress() {
  std::vector<std::thread> threads;
  // Launoch a group of threads
  for (size_t i = 0; i < thread_cnt; ++i) {
    threads.push_back(std::thread(execute, i));
    printf("thread %d start. \n", i);
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
    idx = rand() % table_limit;
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
     << "INDEX L_ORDER_PART (L_ORDERKEY, L_PARTKEY),"
     << "INDEX L_ORDER  (L_ORDERKEY),"
     << "INDEX L_ORDER_SUPP (L_ORDERKEY, L_SUPPKEY),"
     << "INDEX PART (L_PARTKEY),"
     << "INDEX PART_ORDER (L_PARTKEY, L_ORDERKEY),"
     << "INDEX PART_SUPP (L_PARTKEY, L_SUPPKEY),"
     << "INDEX SUPP (L_SUPPKEY),"
     << "INDEX SUPP_ORDER (L_SUPPKEY, L_ORDERKEY),"
     << "INDEX SUPP_PART (L_SUPPKEY, L_PARTKEY));";
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
  int idx = rand() % table_limit;
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

void AlterTable() {
  int idx = rand() % table_limit;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(idx);
  Mysql client;
  if (!client.connect()) {
    printf("AlterTable(): conn failed\n");
    return;
  }
  printf("Alter table: terark_%d\n", idx);
  {
    string stmt = "create index ORDER_LINE on " + table + " (L_ORDERKEY, L_LINENUMBER);";
    AlterExecute(client, stmt);
    printf("Alter table: terark_%d, create index ORDER_LINE done\n", idx);
  }
  {
    string stmt = "drop index PART on " + table + ";";
    AlterExecute(client, stmt);
    printf("Alter table: terark_%d, drop PART done\n", idx);
  }
  {
    string stmt = "drop index PART_ORDER on " + table + ";";
    AlterExecute(client, stmt);
    printf("Alter table: terark_%d, drop PART_ORDER done\n", idx);
  }
  {
    string stmt = "create index PART on " + table + " (L_PARTKEY);";
    AlterExecute(client, stmt);
    printf("Alter table: terark_%d, create index PART done\n", idx);
  }
  {
    string stmt = "create index PART_ORDER on " + table + "(L_PARTKEY, L_ORDERKEY);";
    AlterExecute(client, stmt);
    printf("Alter table: terark_%d, create composite index PART_ORDER done\n", idx);
  }
  release_lock(table);
  printf("done Alter table: terark_%d\n", idx);
}

void AlterExecute(Mysql& client, const std::string& stmt) {
  MYSQL_STMT* m_stmt = client.prepare(stmt);
  client.execute(m_stmt);
  client.release_stmt(m_stmt);
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
  int idx = rand() % table_limit;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(idx);

  string str_stmt = "Insert into " + table +
    " values(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
  MYSQL_STMT* stmt = client.prepare(str_stmt);
  int row_start = rand() % contents.size();
  int limit = min<size_t>(insert_cnt, contents.size() - row_start + 1);
  printf("Insert table: terark_%d, cnt %d\n", idx, limit);
  for (int cnt = 0; cnt < insert_cnt; cnt++) {
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
  Mysql client;
  if (!client.connect()) {
    printf("Delete(): conn failed\n");
    return;
  }
  int idx = rand() % table_limit;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(idx);
  /*{
    string stmt = "delete from " + table +
      " where L_ORDERKEY = ? and L_PARTKEY = ?";
    QueryExecute(client, stmt, kOrderKey, kPartKey);
  }
  {
    string stmt = "delete from " + table +
      " where id = ?";
    QueryExecute(client, stmt, -1, -1);
    }*/
  release_lock(table);
  printf("done Delete: terark_%d\n", idx);
}

// query with primary key? or secondary key
void Query() {
  Mysql client;
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  //int idx = rand() % table_limit;
  int idx = rand() % table_limit + 1;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  //CreateTable(idx);

  {
    string str_stmt = "select * from " + table +
      " where L_ORDERKEY = ? and L_PARTKEY = ?";
    static MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecute(client, stmt, kOrderKey, kPartKey);
  }
  {
    string str_stmt = "select * from " + table +
      " where L_SUPPKEY = ? and L_PARTKEY = ?";
    static MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecute(client, stmt, kSuppKey, kPartKey);
  }
/*
  {
    string stmt = "select * from " + table +
      " where id = ?";
    QueryExecute(client, stmt, -1, -1);
  }
*/
  {
    string str_stmt = "select * from " + table +
      " where L_ORDERKEY = ?";
    static MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecute(client, stmt, kOrderKey, -1);
  }
  release_lock(table);

  total_counts += insert_cnt * 3;
  //printf("done Query table: %s%d\n", TablePrefix.c_str(), idx);
}

/*void Query(Mysql& client, MYSQL_STMT* stmt, const string& table) {
  {
    QueryExecute(client, stmt, table, kOrderKey, kPartKey);
  }
  {
    QueryExecute(client, stmt, table, kSuppKey, kPartKey);
  }
  {
    QueryExecute(client, stmt, table, kOrderKey, -1);
  }
  //release_lock(table);

  total_counts += insert_cnt * 3;
  }*/

/*void QueryExecute(Mysql& client, MYSQL_STMT* stmt, const std::string& table, int idx1, int idx2) {
  int row_start = rand() % contents.size();
  int limit = min<size_t>(insert_cnt, contents.size() - row_start + 1);
  //printf("table: stmt %s, cnt %d\n", str.c_str(), limit);
  for (int cnt = 0; cnt < insert_cnt; cnt++) {
    if (row_start + cnt >= contents.size())
      break;
    string& line = contents[cnt + row_start];
    std::vector<std::string> results;
    boost::split(results, line, [](char c){return c == '|';});
    if (idx1 != -1 && idx2 != -1) {
      MYSQL_BIND in_params[2];
      client.bind_arg(in_params[0], stoi(results[idx1]));
      client.bind_arg(in_params[1], stoi(results[idx2]));
      client.bind_execute(stmt, in_params);
    } else if (idx1 != -1) {
      MYSQL_BIND in_params[1];
      client.bind_arg(in_params[0], stoi(results[idx1]));
      client.bind_execute(stmt, in_params);
    } else {
      MYSQL_BIND in_params[1];
      client.bind_arg(in_params[0], cnt + row_start);
      client.bind_execute(stmt, in_params);
    }
  }
  client.release_stmt(stmt);
  }*/



void QueryExecute(Mysql& client, MYSQL_STMT* stmt, int idx1, int idx2) {
  int row_start = rand() % contents.size();
  int limit = min<size_t>(insert_cnt, contents.size() - row_start + 1);
  //printf("table: stmt %s, cnt %d\n", str.c_str(), limit);
  for (int cnt = 0; cnt < insert_cnt; cnt++) {
    if (row_start + cnt >= contents.size())
      break;
    string& line = contents[cnt + row_start];
    std::vector<std::string> results;
    boost::split(results, line, [](char c){return c == '|';});
    if (idx1 != -1 && idx2 != -1) {
      MYSQL_BIND in_params[2];
      client.bind_arg(in_params[0], stoi(results[idx1]));
      client.bind_arg(in_params[1], stoi(results[idx2]));
      client.bind_execute(stmt, in_params);
    } else if (idx1 != -1) {
      MYSQL_BIND in_params[1];
      client.bind_arg(in_params[0], stoi(results[idx1]));
      client.bind_execute(stmt, in_params);
    } else {
      MYSQL_BIND in_params[1];
      client.bind_arg(in_params[0], cnt + row_start);
      client.bind_execute(stmt, in_params);
    }
  }
  client.release_stmt(stmt);
}
