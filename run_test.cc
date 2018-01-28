
#include <time.h>
#include <ctime>
#include <ratio>

#include <atomic>
#include <chrono>
#include <functional>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <random>       // std::default_random_engine
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

//#include <boost/algorithm/string.hpp>
//#include <boost/utility/string_ref.hpp>

#include <terark/fstring.cpp>
#include <terark/lcast.cpp>
#include "client.h"
#include "run_test.h"

using namespace std;
using namespace std::chrono;
using namespace terark;

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
//atomic<high_resolution_clock::time_point> total_elapse = ;
//time_t start_tm = time(0);

size_t thread_cnt = terark::getEnvLong("threadCount", 4);
size_t table_cnt = terark::getEnvLong("tableCount", 16);
size_t row_cnt = terark::getEnvLong("insertCount", 1);
int    test_type = terark::getEnvLong("testType", 5); // kQuery as default

enum test_type_t {
  kStressTest        = 0,
  kInsertBulkTest    = 1,
  kQueryTest,
  kQueryPreparedTest,
  kInsertRandomTest  = 4,
  kUpdateRandomTest
};

enum op_type_t {
  kCreateTable = 0,
  kDropTable,
  kAlterTable,
  kInsert,
  kDelete,
  kUpdate,
  kQuery
};

enum query_t {
  kOrder_Part = 0,
  kSupp_Part,
  kOrder
};
typedef map<int, vector<MYSQL_STMT*>> I2PreparedStmts;
typedef map<int, string> I2StrStmt;

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

//string TablePrefix = "terark_";
string TablePrefix = "lineitem";

void CreateTable(int);
void DropTable();
void AlterTable();
void Insert();
void Delete();
void Query();

void InsertBulk(Mysql& client, int table_idx, int offset, int round_cnt);
void Update(Mysql& client, int table_idx, int offset, int round_cnt);
void QueryExecute(Mysql& client, const std::string& str_in, int idx1, int idx2);
void QueryExecutePrepared(Mysql& client, MYSQL_STMT* stmt, int idx1, int idx2);
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
  if (test_type == kInsertRandomTest ||
      test_type == kUpdateRandomTest) {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(contents.begin(), contents.end(), std::default_random_engine(seed));
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
  }
}

void prepare_stmts(Mysql& client, I2PreparedStmts& pStmts) {
  for (int idx = 1; idx <= table_cnt; idx++) {
    string table = TablePrefix + to_string(idx);
    {
      string str_stmt = "select * from " + table +
	" where L_ORDERKEY = ? and L_PARTKEY = ?";
      MYSQL_STMT* stmt = client.prepare(str_stmt);
      pStmts[kOrder_Part].push_back(stmt);
    }
    {
      string str_stmt = "select * from " + table +
	" where L_SUPPKEY = ? and L_PARTKEY = ?";
      MYSQL_STMT* stmt = client.prepare(str_stmt);
      pStmts[kSupp_Part].push_back(stmt);
    }
    {
      string str_stmt = "select * from " + table +
	" where L_ORDERKEY = ?";
      MYSQL_STMT* stmt = client.prepare(str_stmt);
      pStmts[kOrder].push_back(stmt);
    }
  }
}

MYSQL_STMT* gen_stmt(Mysql& client, query_t sel) {
  int idx = rand() % table_cnt + 1;
  string table = TablePrefix + to_string(idx);
  string str_stmt;
  switch (sel) {
  case kOrder_Part:
    return 0;
  case kSupp_Part:
    return 0;
  case kOrder:
    return 0;
    /*
  case 4:
    printf("will query orderkey > and partkey < \n");
    str_stmt = "select * from " + table +
      " where L_ORDERKEY > ? and L_PARTKEY < ?";
    return client.prepare(str_stmt);
  case 5:
    printf("will query orderkey < \n");
    str_stmt = "select * from " + table +
      " where L_ORDERKEY > ?";
    return client.prepare(str_stmt);
  case 6:
    printf("will query orderkey < and partkey > \n");
    str_stmt = "select * from " + table +
      " where L_ORDERKEY < ? and L_PARTKEY > ?";
    return client.prepare(str_stmt);
    */
  default:
    break;
  }
  return 0;
}

void execute_upsert(int thread_idx) {
  Mysql client;
  if (!client.connect()) {
    printf("ExecuteInsert(): conn failed\n");
    return;
  }
  int round_cnt = 0, tick = 0;
  string test_t;
  if (test_type == kInsertBulkTest) {
    tick = 20;
    round_cnt = 100;
    test_t = "[InsertBulk] ";
  } else if (test_type == kInsertRandomTest) {
    tick = 3000;
    round_cnt = 10;
    test_t = "[InsertRandom] ";
  } else if (test_type == kUpdateRandomTest) {
    tick = 3000;
    round_cnt = 10;
    test_t = "[UpdateRandom] ";
  }
  const int limit = table_cnt / thread_cnt;
  const int start_table = limit * thread_idx;
  for (int cnt = 0; cnt < limit; cnt++) {
    int table_idx = start_table + cnt;
    for (int offset = 0; offset < contents.size(); offset += round_cnt) {
      high_resolution_clock::time_point start = high_resolution_clock::now();
      if (test_type == kUpdateRandomTest)
	Update(client, table_idx, offset, round_cnt);
      else
	InsertBulk(client, table_idx, offset, round_cnt);
      high_resolution_clock::time_point end = high_resolution_clock::now();
      duration<int,std::micro> time_span = duration_cast<duration<int,std::micro>>(end - start);
      total_elapse += time_span.count();
      total_rounds ++;
      if (total_rounds.load() % tick == 0) {
	printf("== %s, TPS %f, insert %lld, time elapse %f sec\n", 
	       test_t.c_str(), (double)thread_cnt * total_counts.load() * 1e6 / total_elapse.load(), 
	       total_counts.load(), total_elapse.load() / 1e6 / thread_cnt);
	total_counts = 0;
	total_elapse = 0;
	total_rounds = 0;
      }
    }
  }
}

void execute_query(int tid) {
  Mysql client;
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  int cycle = 0;
  while (true) {
    high_resolution_clock::time_point start = high_resolution_clock::now();
    int idx = rand() % table_cnt;
    string table = TablePrefix + to_string(idx);
    if (cycle == 0) {
      string str_stmt = "select * from " + table +
	" where L_ORDERKEY = ? and L_PARTKEY = ?";
      QueryExecute(client, str_stmt, kOrderKey, kPartKey);
    } else if (cycle == 1) {
      string str_stmt = "select * from " + table +
	" where L_SUPPKEY = ? and L_PARTKEY = ?";
      QueryExecute(client, str_stmt, kSuppKey, kPartKey);
    } else if (cycle == 2) {
      string str_stmt = "select * from " + table +
	" where L_ORDERKEY = ?";
      QueryExecute(client, str_stmt, kOrderKey, -1);
    }
    cycle = (cycle + 1) % 3;
    total_counts += row_cnt;
    total_rounds++;
    high_resolution_clock::time_point end = high_resolution_clock::now();
    duration<int,std::micro> time_span = duration_cast<duration<int,std::micro>>(end - start);
    total_elapse += time_span.count();
    if (total_rounds.load() % 30001 == 0) {
      printf("== QPS %f, query %lld, time elapse %f sec\n", 
	     (double)thread_cnt * total_counts.load() * 1e6 / total_elapse.load(), 
	     total_counts.load(), total_elapse.load() / 1e6 / thread_cnt);
      total_rounds = 1;
      total_counts = 0;
      total_elapse = 0;
    }
  }
}

void execute_query_prepared(int tid) {
  Mysql client;
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  I2PreparedStmts stmts;
  prepare_stmts(client, stmts);
  int cycle = 0;
  while (true) {
    high_resolution_clock::time_point start = high_resolution_clock::now();
    int idx = rand() % table_cnt;
    MYSQL_STMT* stmt = stmts[(query_t)cycle][idx];
    if (cycle == 0) {
      QueryExecutePrepared(client, stmt, kOrderKey, kPartKey);
    } else if (cycle == 1) {
      QueryExecutePrepared(client, stmt, kSuppKey, kPartKey);
    } else if (cycle == 2) {
      QueryExecutePrepared(client, stmt, kOrderKey, -1);
    }

    /*if (cycle == 4) { // '>' '<' part test
      stmt = gen_stmt(client, 4);
      start = time(0);
      QueryExecutePrepared(client, stmt, kOrderKey, kPartKey);
    } else if (cycle == 5) {
      stmt = gen_stmt(client, 2);
      start = time(0);
      QueryExecutePrepared(client, stmt, kOrderKey, -1);
      //printf("done query orderkey < \n");
    } else if (cycle == 6) {
      stmt = gen_stmt(client, 2);
      start = time(0);
      QueryExecutePrepared(client, stmt, kOrderKey, kPartKey);
      }*/

    cycle = (cycle + 1) % 3;
    total_counts += row_cnt;
    total_rounds++;
    high_resolution_clock::time_point end = high_resolution_clock::now();
    duration<int,std::micro> time_span = duration_cast<duration<int,std::micro>>(end - start);
    total_elapse += time_span.count();
    if (total_rounds.load() % 30001 == 0) {
      printf("== QPS %f, query %lld, time elapse %f sec\n", 
	     (double)thread_cnt * total_counts.load() * 1e6 / total_elapse.load(), 
	     total_counts.load(), total_elapse.load() / 1e6 / thread_cnt);
      total_rounds = 1;
      total_counts = 0;
      total_elapse = 0;
    }
  }
}


void StartStress() {
  std::vector<std::thread> threads;
  // Launoch a group of threads
  for (size_t i = 0; i < thread_cnt; ++i) {
    switch (test_type) {
    case kStressTest:
      threads.push_back(std::thread(execute, i));
      break;
    case kInsertBulkTest:
      threads.push_back(std::thread(execute_upsert, i));
      break;
    case kQueryTest:
      threads.push_back(std::thread(execute_query, i));
      break;
    case kQueryPreparedTest:
      threads.push_back(std::thread(execute_query_prepared, i));
      break;
    case kInsertRandomTest:
      threads.push_back(std::thread(execute_upsert, i));
      break;
    case kUpdateRandomTest:
      threads.push_back(std::thread(execute_upsert, i));
      break;
    }
    
    //threads.push_back(std::thread(execute_query_prepared, i));
    //
    
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
    idx = rand() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;

  printf("CreateTable: %s%d\n", TablePrefix.c_str(), idx);
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
  printf("done CreateTable: %s%d\n", TablePrefix.c_str(), idx);
}

void DropTable() {
  int idx = rand() % table_cnt;
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
  int idx = rand() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(idx);
  Mysql client;
  if (!client.connect()) {
    printf("AlterTable(): conn failed\n");
    return;
  }
  printf("Alter table: %s%d\n", TablePrefix.c_str(), idx);
  {
    string stmt = "create index ORDER_LINE on " + table + " (L_ORDERKEY, L_LINENUMBER);";
    AlterExecute(client, stmt);
    printf("Alter table: %s%d, create index ORDER_LINE done\n", TablePrefix.c_str(), idx);
  }
  {
    string stmt = "drop index PART on " + table + ";";
    AlterExecute(client, stmt);
    printf("Alter table: %s%d, drop PART done\n", TablePrefix.c_str(), idx);
  }
  {
    string stmt = "drop index PART_ORDER on " + table + ";";
    AlterExecute(client, stmt);
    printf("Alter table: %s%d, drop PART_ORDER done\n", TablePrefix.c_str(), idx);
  }
  {
    string stmt = "create index PART on " + table + " (L_PARTKEY);";
    AlterExecute(client, stmt);
    printf("Alter table: %s%d, create index PART done\n", TablePrefix.c_str(), idx);
  }
  {
    string stmt = "create index PART_ORDER on " + table + "(L_PARTKEY, L_ORDERKEY);";
    AlterExecute(client, stmt);
    printf("Alter table: %s%d, create composite index PART_ORDER done\n", TablePrefix.c_str(), idx);
  }
  release_lock(table);
  printf("done Alter table: %s%d\n", TablePrefix.c_str(), idx);
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
  int idx = rand() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(idx);

  string str_stmt = "Insert into " + table +
    " values(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
  MYSQL_STMT* stmt = client.prepare(str_stmt);
  int row_start = rand() % contents.size();
  int limit = min<size_t>(row_cnt, contents.size() - row_start + 1);
  std::vector<fstring> results;
  printf("Insert table: %s%d, cnt %d\n", TablePrefix.c_str(), idx, limit);
  for (int cnt = 0; cnt < row_cnt; cnt++) {
    if (row_start + cnt >= contents.size())
      break;

    const string& line = contents[cnt + row_start];
    results.resize(0);
    fstring(line).split('|', &results);

    MYSQL_BIND in_params[17];
    memset(in_params, 0, sizeof(in_params));

    client.bind_arg(in_params[0], atoi(results[kOrderKey].data()));
    client.bind_arg(in_params[1], atoi(results[kPartKey].data()));

    client.bind_arg(in_params[2], atoi(results[kSuppKey].data()));
    client.bind_arg(in_params[3], atoi(results[kLineNumber].data()));

    client.bind_arg(in_params[4], atof(results[kQuantity].data()));
    client.bind_arg(in_params[5], atof(results[kExtendedPrice].data()));
    client.bind_arg(in_params[6], atof(results[kDiscount].data()));
    client.bind_arg(in_params[7], atof(results[kTax].data()));

    client.bind_arg(in_params[8], results[kReturnFlag].data(), 1);
    client.bind_arg(in_params[9], results[kLineStatus].data(), 1);

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
  printf("done Insert table: %s%d\n", TablePrefix.c_str(), idx);
}

void InsertBulk(Mysql& client, int table_idx, int offset, int round_cnt) {
  string table = TablePrefix + to_string(table_idx);
  std::vector<fstring> results;
  for (int i = 0; i < round_cnt; i++) {
    if (offset + i >= contents.size())
      continue;
    const string& line = contents[offset + i];
    results.resize(0);
    fstring(line).split('|', &results);
    stringstream sst;
    sst << "Insert into " << table << " values("
	<< results[kOrderKey].str() << ", "
	<< results[kPartKey].str() << ", "
	<< results[kSuppKey].str() << ", "
	<< results[kLineNumber].str() << ", "
	<< results[kQuantity].str() << ", "
	<< results[kExtendedPrice].str() << ", "
	<< results[kDiscount].str() << ", "
	<< results[kTax].str() << ", "
	<< "'"  << results[kReturnFlag].str()   << "'"  << ", "
	<< "'"  << results[kLineStatus].str()   << "'"  << ", "
	<< "\"" << results[kShipDate].str() << "\"" << ", "
      	<< "\"" << results[kCommitDate].str() << "\"" << ", "
	<< "\"" << results[kRecepitDate].str() << "\"" << ", "
	<< "\"" << results[kShipinStruct].str() << "\"" << ", "
	<< "\"" << results[kShipMode].str()     << "\"" << ", "
	<< "\"" << results[kComment].str()      << "\"" << ")";

    client.execute(sst.str());
    total_counts += 1;
  }
  //printf("done Insert table: %s%d\n", TablePrefix.c_str(), table_idx);
}

void Update(Mysql& client, int table_idx, int offset, int round_cnt) {
  string table = TablePrefix + to_string(table_idx);
  std::vector<fstring> results;
  for (int i = 0; i < round_cnt; i++) {
    if (offset + i >= contents.size())
      continue;
    const string& line = contents[offset + i];
    results.resize(0);
    fstring(line).split('|', &results);
    stringstream sst;
    int sup = atoi(results[kSuppKey].data()) * (offset - round_cnt) + round_cnt;
    int lin = atoi(results[kLineNumber].data()) * offset - round_cnt;
    fstring comm = results[kComment];
    if (comm.size() > 100)
      comm = comm.substr(1);
    sst << "Update " << table << " set "
	<< " L_SUPPKEY=" << to_string(sup) << ", "
	<< " L_LINENUMBER=" << to_string(lin) << ", "
	<< " L_COMMENT=" << "\"" << comm.str()      << "\"" << " "
	<< "where kOrderKey = " << results[kOrderKey].str() << " and "
	<< "kPartKey = " << results[kPartKey].str();

    printf("%s\n", sst.str().c_str());
    //client.execute(sst.str());
    //total_counts += 1;
  }
}


void Delete() {
  Mysql client;
  if (!client.connect()) {
    printf("Delete(): conn failed\n");
    return;
  }
  int idx = rand() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(idx);
  {
    string str_stmt = "delete from " + table +
      " where L_ORDERKEY = ? and L_PARTKEY = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(client, stmt, kOrderKey, kPartKey);
  }
  {
    string str_stmt = "delete from " + table +
      " where id = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(client, stmt, -1, -1);
  }
  release_lock(table);
  printf("done Delete: %s%d\n", TablePrefix.c_str(), idx);
}

// query with primary key? or secondary key
void Query() {
  Mysql client;
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  //int idx = rand() % table_cnt;
  int idx = rand() % table_cnt + 1;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(idx);

  {
    string str_stmt = "select * from " + table +
      " where L_ORDERKEY = ? and L_PARTKEY = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(client, stmt, kOrderKey, kPartKey);
  }
  {
    string str_stmt = "select * from " + table +
      " where L_SUPPKEY = ? and L_PARTKEY = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(client, stmt, kSuppKey, kPartKey);
  }
  {
    string str_stmt = "select * from " + table +
      " where id = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(client, stmt, -1, -1);
  }
  {
    string str_stmt = "select * from " + table +
      " where L_ORDERKEY = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(client, stmt, kOrderKey, -1);
  }
  release_lock(table);

  printf("done Query table: %s%d\n", TablePrefix.c_str(), idx);
}

void replace_with(string& str, const string& from, const string& to) {
  size_t pos = str.find(from);
  while (pos != string::npos) {
    str.replace(pos, from.size(), to);
    pos = str.find(from);
  }
}
void QueryExecute(Mysql& client, const std::string& str_in, int idx1, int idx2) {
  int row_start = rand() % contents.size();
  int limit = min<size_t>(row_cnt, contents.size() - row_start + 1);
  std::vector<fstring> results;
  for (int cnt = 0; cnt < row_cnt; cnt++) {
    if (row_start + cnt >= contents.size())
      break;
    const string& line = contents[cnt + row_start];
    results.resize(0);
    fstring(line).split('|', &results);
    string str_stmt = str_in;
    if (idx1 != -1 && idx2 != -1) {
      replace_with(str_stmt, "?", results[idx1].c_str());
      replace_with(str_stmt, "?", results[idx2].c_str());
      cout << str_stmt << endl; //
      //client.execute(str_stmt);
    } else if (idx1 != -1) {
      replace_with(str_stmt, "?", results[idx1].c_str());
      cout << str_stmt << endl; //
      //client.execute(str_stmt);
    }
    // consume data
    //MYSQL_RES *res = client.use_result();
    //client.free_result(res);
  }
}

void QueryExecutePrepared(Mysql& client, MYSQL_STMT* stmt, int idx1, int idx2) {
  int row_start = rand() % contents.size();
  int limit = min<size_t>(row_cnt, contents.size() - row_start + 1);
  //printf("table: stmt %s, cnt %d\n", str.c_str(), limit);
  std::vector<fstring> results;
  for (int cnt = 0; cnt < row_cnt; cnt++) {
    if (row_start + cnt >= contents.size())
      break;
    const string& line = contents[cnt + row_start];
    results.resize(0);
    fstring(line).split('|', &results);
    if (idx1 != -1 && idx2 != -1) {
      MYSQL_BIND in_params[2];
      client.bind_arg(in_params[0], atoi(results[idx1].data()));
      client.bind_arg(in_params[1], atoi(results[idx2].data()));
      client.bind_execute(stmt, in_params);
    } else if (idx1 != -1) {
      MYSQL_BIND in_params[1];
      client.bind_arg(in_params[0], atoi(results[idx1].data()));
      client.bind_execute(stmt, in_params);
    } else {
      MYSQL_BIND in_params[1];
      client.bind_arg(in_params[0], cnt + row_start);
      client.bind_execute(stmt, in_params);
    }
    client.consume_data(stmt);
  }
  //client.release_stmt(stmt);
}
