
#include <sys/time.h>
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

#include <boost/algorithm/string.hpp>
#include <boost/utility/string_ref.hpp>

#include "client.h"
#include "run_test.h"

using namespace std;
using namespace std::chrono;

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

struct Context {
  Context() : mt(std::hash<std::thread::id>{}(std::this_thread::get_id())) {}
  std::mt19937_64 mt;
};

atomic<long> total_counts = { 0 };
atomic<long> total_rounds = { 0 };
atomic<long> total_elapse = { 0 };
//atomic<high_resolution_clock::time_point> total_elapse = ;
//time_t start_tm = time(0);

size_t thread_cnt = 32;
size_t table_cnt = 100;
size_t row_cnt = 1;
std::vector<size_t> cycle_item = {0, 1, 2};
int    test_type = 2; // kQuery as default

enum test_type_t {
  kStressTest        = 0,
  kInsertBulkTest    = 1,
  kQueryTest,
  kQueryPreparedTest,
  kInsertRandomTest  = 4,
  kUpdateRandomTest,
  kVerifyData = 6,
  kPrepareTable,
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
  kPart_Larger,
  kPart_Smaller,
  kSupp_Smaller
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

void CreateTable(Context&, int);
void DropTable(Context&);
void AlterTable(Context&);
void Insert(Context&);
void Delete(Context&);
void Query(Context&);

void InsertBulk(Context&, Mysql& client, int table_idx, int offset, int round_cnt);
void Update(Context&, Mysql& client, int table_idx, int offset, int round_cnt);
void QueryExecute(Context&, Mysql& client, const std::string& str_in, int idx1, int idx2);
void QueryExecutePrepared(Context&, Mysql& client, MYSQL_STMT* stmt, int idx1, int idx2);
MYSQL_RES* QueryExecuteAndReturn(Context&, Mysql& client, const std::string& str_in, 
                                 int offset, int idx1, int idx2);
void AlterExecute(Context&, Mysql& client, const std::string& stmt);

class RandomIndex {
public:
  void init(size_t _T, size_t _N) {
    N = _N;
    T.reverse(_T);
    random_device rd;
    mt.seed(rd());
    for (size_t i = 0; i < _T; ++i) {
      Item item;
      item.t = i;
      item.i = mt() % N;
      item.c = 0;
      item.mod = get_prime(mt() & 0xFFFFFFFFFFFFULL + 5 * N);
      T.push_back(item);
    }
  }
  void get(size_t count, std::vector<std::pair<int, int>>& vec) const {
    vec.clear();
    std::unique_lock<std::mutex> l(M);
    for (size_t i = 0; i < count && T.empty(); ++i) {
      auto& item = T[mt() % T.size()];
      item.i += mod;
      item.i %= N;
      ++item.c;
      vec.emplace_back(int(item.t), int(item.i));
      if (item.c == N) {
        item = T.back();
        T.pop_back();
      }
    }
  }
  
private:
  bool is_prime(size_t candidate) {
    if((candidate & 1) != 0) {
      size_t limit = size_t(std::sqrt(candidate));
      for(size_t divisor = 3; divisor <= limit; divisor += 2) {
        if((candidate % divisor) == 0) {
            return false;
        }
      }
      return true;
    }
    return (candidate == 2);
  }

  size_t get_prime(size_t size) {
    static size_t const prime_array[] =
    {
        7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
        1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
        17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
        187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
        1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
    };
    for(auto prime : prime_array) {
        if(prime >= size) {
            return prime;
        }
    }
    for(size_t prime = (size | 1); prime < std::numeric_limits<uint64_t>::max(); prime += 2) {
        if(is_prime(prime) && ((prime - 1) % 101 != 0)) {
            return prime;
        }
    }
    return size;
  }
  struct Item {
    size_t t;
    size_t i;
    size_t c;
    size_t mod;
  }
  size_t N;
  std::vector<Item> T;
  std::mt19937_64 mt;
  std::mutex M;
} random_index;

void Init(const string& inpath) {
  {
    char* pth_cnt = getenv("threadCount");
    if (pth_cnt)
      thread_cnt = atoi(pth_cnt);
    assert(thread_cnt < 1000);
  }
  {
    char* pta_cnt = getenv("tableCount");
    if (pta_cnt)
      table_cnt = atoi(pta_cnt);
    assert(table_cnt < 1000);
  }
  {
    char* pt_type = getenv("testType");
    if (pt_type) {
      if (pt_type == std::string("InsertBulk"))
        test_type = kInsertBulkTest;
      else if (pt_type == std::string("Query"))
        test_type = kQueryTest;
      else if (pt_type == std::string("QueryPrepared"))
        test_type = kQueryPreparedTest;
      else if (pt_type == std::string("Insert"))
        test_type = kInsertRandomTest;
      else if (pt_type == std::string("Update"))
        test_type = kUpdateRandomTest;
      else if (pt_type == std::string("PrepareTable"))
        test_type = kPrepareTable;
      else if (pt_type == std::string("Verify"))
        test_type = kVerifyData;
      else
        test_type = atoi(pt_type);
    }
    assert(0 < test_type && test_type < 8);
  }
  {
    char* q_type = getenv("queryType");
    if (q_type) {
      int q_len = strlen(q_type);
      std::vector<size_t> new_cycle_item;
      for (int i = 0; i < q_len; ++i) {
        switch (q_type[i]) {
        case 'p': case 'P':
          new_cycle_item.push_back(0);
          break;
        case 'i': case 'I':
          new_cycle_item.push_back(1);
          break;
        case 'r': case 'R':
          new_cycle_item.push_back(2);
          break;
        }
      }
      if (!new_cycle_item.empty()) {
        new_cycle_item.swap(cycle_item);
      }
    }
  }

  srand (time(NULL));
  // read in data
  string path = "./lineitem_2m.tbl";
  if (!inpath.empty())
    path = inpath;
  ifstream in(path.c_str());
  assert(in);
  string line;
  while (getline(in, line)) {
    if (line.size() < 10)
      continue;
    contents.push_back(line);
  }
  in.close();
  if (test_type == kInsertRandomTest ||
      test_type == kUpdateRandomTest) {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(contents.begin(), contents.end(), std::default_random_engine(seed));
  }
  if (test_type == kVerifyData) {
    char* cport = getenv("ref_port");
    if (!cport) {
      printf("make sure you'v provide both 'port' and 'ref_port'\n");
      exit(-1);
    }
    random_index.init(table_cnt, contents.size());
  }
}

void execute(int tid) {
  Context context;
  while (true) {
    int type = rand() % 7;
    switch (type) {
    case kCreateTable:
      CreateTable(context, -1);
      break;
    case kDropTable:
      DropTable(context);
      break;
    case kAlterTable:
      AlterTable(context);
      break;
    case kInsert:
      Insert(context);
      break;
    case kDelete:
      Delete(context);
      break;
    case kQuery:
      Query(context);
      break;
    }
  }
}

void prepare_stmts(Mysql& client, I2PreparedStmts& pStmts) {
  for (int idx = 0; idx < table_cnt; idx++) {
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
        " where L_PARTKEY > ? limit 1";
      MYSQL_STMT* stmt = client.prepare(str_stmt);
      pStmts[kPart_Larger].push_back(stmt);
    }
    {
      string str_stmt = "select * from " + table +
        " where L_PARTKEY < ? limit 1";
      MYSQL_STMT* stmt = client.prepare(str_stmt);
      pStmts[kPart_Smaller].push_back(stmt);
    }
    {
      string str_stmt = "select * from " + table +
        " where L_SUPPKEY < ? limit 1";
      MYSQL_STMT* stmt = client.prepare(str_stmt);
      pStmts[kSupp_Smaller].push_back(stmt);
    }
  }
}

void execute_prepare(Context& context) {
  Mysql client;
  if (!client.connect()) {
    printf("Prepare(): conn failed\n");
    return;
  }
  for (int cnt = 0; cnt < table_cnt; ++cnt) {
    CreateTable(context, cnt);
  }
}

void execute_update(int idx) {
  Context context;
  Mysql client;
  if (!client.connect()) {
    printf("ExecuteInsert(): conn failed\n");
    return;
  }
  int round_cnt = 1, tick = 10000;
  string test_t = "[UpdateRandom] ";
  while (true) {
    int table_idx = context.mt() % table_cnt;
    int offset = context.mt() % contents.size();
    high_resolution_clock::time_point start = high_resolution_clock::now();
    Update(context, client, table_idx, offset, round_cnt);
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

void execute_insert(int thread_idx) {
  Context context;
  Mysql client;
  if (!client.connect()) {
    printf("ExecuteInsert(): conn failed\n");
    return;
  }
  int round_cnt = 100, tick = 200;
  string test_t = "[InsertBulk] ";
  const int limit = table_cnt / thread_cnt;
  const int start_table = limit * thread_idx;
  for (int cnt = 0; cnt < limit; cnt++) { // skip table 0
    int table_idx = start_table + cnt;
    for (int offset = 0; offset < contents.size(); offset += round_cnt) {
      high_resolution_clock::time_point start = high_resolution_clock::now();
      InsertBulk(context, client, table_idx, offset, round_cnt);
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

void execute_insert_random(int thread_idx) {
  Context context;
  Mysql client;
  if (!client.connect()) {
    printf("ExecuteInsert(): conn failed\n");
    return;
  }
  int round_cnt = 100, tick = 200;
  size_t bulk = 10000;
  string test_t = "[InsertRandom] ";
  std::vector<std::pair<size_t, size_t>> vec;
  while (true) {
    random_index.get(bulk, vec);
    if (vec.empty())
      break;
    for (auto pair : vec) {
      size_t table_idx = pair.first;
      size_t offset = pair.second;
      high_resolution_clock::time_point start = high_resolution_clock::now();
      InsertBulk(context, client, table_idx, offset, round_cnt);
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
  Context context;
  Mysql client;
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  int cycle = 0;
  while (true) {
    high_resolution_clock::time_point start = high_resolution_clock::now();
    int idx = context.mt() % table_cnt;
    string table = TablePrefix + to_string(idx);
    string str_stmt;
    switch (cycle_item[cycle]) {
    case 0:
      str_stmt = "select * from " + table +
        " where L_ORDERKEY = ? and L_PARTKEY = ?";
      QueryExecute(context, client, str_stmt, kOrderKey, kPartKey);
      break;
    case 1:
      str_stmt = "select * from " + table +
        " where L_SUPPKEY = ? and L_PARTKEY = ?";
      QueryExecute(context, client, str_stmt, kSuppKey, kPartKey);
      break;
    case 2:
      switch (context.mt() % 3) {
      case 0:
        str_stmt = "select * from " + table +
          " where L_PARTKEY > ? limit 1";
        QueryExecute(context, client, str_stmt, kPartKey, -1);
        break;
      case 1:
        str_stmt = "select * from " + table +
          " where L_PARTKEY < ? limit 1";
        QueryExecute(context, client, str_stmt, kPartKey, -1);
        break;
      case 2:
        str_stmt = "select * from " + table +
          " where L_SUPPKEY < ? limit 1";
        QueryExecute(context, client, str_stmt, kSuppKey, -1);
        break;
      }
      break;
    }
    cycle = (cycle + 1) % cycle_item.size();
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
  Context context;
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
    int idx = context.mt() % table_cnt;
    switch (cycle_item[cycle]) {
    case 0:
      QueryExecutePrepared(context, client, stmts[0][idx], kOrderKey, kPartKey);
      break;
    case 1:
      QueryExecutePrepared(context, client, stmts[1][idx], kSuppKey, kPartKey);
      break;
    case 2:
      switch (2 + context.mt() % 3) {
      case 2:
        QueryExecutePrepared(context, client, stmts[2][idx], kPartKey, -1);
        break;
      case 3:
        QueryExecutePrepared(context, client, stmts[3][idx], kPartKey, -1);
        break;
      case 4:
        QueryExecutePrepared(context, client, stmts[4][idx], kSuppKey, -1);
        break;
      }
      break;
    }
    cycle = (cycle + 1) % cycle_item.size();
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

void execute_query_verify(int tid) {
  Context context;
  Mysql client("port");
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  Mysql ref_client("ref_port");
  if (!ref_client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  int cycle = 0;
  while (true) {
    int idx = context.mt() % table_cnt;
    string table = TablePrefix + to_string(idx);
    int offset = context.mt() % contents.size();
    MYSQL_RES *res = nullptr,
      *ref_res = nullptr;
    if (cycle == 0) {
      string str_stmt = "select * from " + table +
        " where L_ORDERKEY = ? and L_PARTKEY = ?";
      res     = QueryExecuteAndReturn(context,     client, str_stmt, offset, kOrderKey, kPartKey);
      ref_res = QueryExecuteAndReturn(context, ref_client, str_stmt, offset, kOrderKey, kPartKey);
    } else if (cycle == 1) {
      string str_stmt = "select * from " + table +
        " where L_SUPPKEY = ? and L_PARTKEY = ? order by L_ORDERKEY, L_PARTKEY limit 1";
      res     = QueryExecuteAndReturn(context,     client, str_stmt, offset, kSuppKey, kPartKey);
      ref_res = QueryExecuteAndReturn(context, ref_client, str_stmt, offset, kSuppKey, kPartKey);
    } else if (cycle == 2) {
      string str_stmt = "select * from " + table +
        " where L_PARTKEY > ? order by L_ORDERKEY, L_PARTKEY limit 1";
      res     = QueryExecuteAndReturn(context,     client, str_stmt, offset, kPartKey, -1);
      ref_res = QueryExecuteAndReturn(context, ref_client, str_stmt, offset, kPartKey, -1);
    } else if (cycle == 3) {
      string str_stmt = "select * from " + table +
        " where L_PARTKEY < ? order by L_ORDERKEY, L_PARTKEY limit 1";
      res     = QueryExecuteAndReturn(context,     client, str_stmt, offset, kPartKey, -1);
      ref_res = QueryExecuteAndReturn(context, ref_client, str_stmt, offset, kPartKey, -1);
    } else if (cycle == 4) {
      string str_stmt = "select * from " + table +
        " where L_SUPPKEY < ?  order by L_ORDERKEY, L_PARTKEY limit 1";
      res     = QueryExecuteAndReturn(context,     client, str_stmt, offset, kSuppKey, -1);
      ref_res = QueryExecuteAndReturn(context, ref_client, str_stmt, offset, kSuppKey, -1);
    }
    Mysql::verify_data(res, ref_res);
    client.free_result(res);
    ref_client.free_result(ref_res);
    cycle = (cycle + 1) % 5;
  }
}


void StartStress() {
  if (test_type == kPrepareTable) {
    Context context;
    execute_prepare(context);
    return;
  }

  std::vector<std::thread> threads;
  std::string testName;
  // Launoch a group of threads
  for (size_t i = 0; i < thread_cnt; ++i) {
    switch (test_type) {
    case kStressTest:
      threads.push_back(std::thread(execute, i));
      break;
    case kInsertBulkTest:
      testName = "[InsertBulk] TPS";
      threads.push_back(std::thread(execute_insert, i));
      break;
    case kQueryTest:
      testName = "[Query] QPS";
      threads.push_back(std::thread(execute_query, i));
      break;
    case kQueryPreparedTest:
      testName = "[QueryPrepared] QPS";
      threads.push_back(std::thread(execute_query_prepared, i));
      break;
    case kInsertRandomTest:
      testName = "[InsertRandom] TPS";
      threads.push_back(std::thread(execute_insert_random, i));
      break;
    case kUpdateRandomTest:
      testName = "[UpdateRandom] TPS";
      threads.push_back(std::thread(execute_update, i));
      break;
    case kVerifyData:
      testName = "[VerifyData] TPS";
      threads.push_back(std::thread(execute_query_verify, i));
      break;
    }
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

void CreateTable(Context& context, int idx) {
  if (idx == -1)
    idx = context.mt() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;

  printf("CreateTable: %s%d\n", TablePrefix.c_str(), idx);
  stringstream ss;
  ss << "("                          //"(id BIGINT NOT NULL AUTO_INCREMENT, "
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
     << "L_SHIPDATE    DATE NOT NULL,"
     << "L_COMMITDATE  DATE NOT NULL,"
     << "L_RECEIPTDATE DATE NOT NULL,"
     << "L_SHIPINSTRUCT CHAR(25) NOT NULL,"
     << "L_SHIPMODE     CHAR(10) NOT NULL,"
     << "L_COMMENT      VARCHAR(512) NOT NULL,"
     << "PRIMARY KEY (L_ORDERKEY, L_PARTKEY),"
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

void DropTable(Context& context) {
  int idx = context.mt() % table_cnt;
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

void AlterTable(Context& context) {
  int idx = context.mt() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(context, idx);
  Mysql client;
  if (!client.connect()) {
    printf("AlterTable(): conn failed\n");
    return;
  }
  printf("Alter table: %s%d\n", TablePrefix.c_str(), idx);
  {
    string stmt = "create index ORDER_LINE on " + table + " (L_ORDERKEY, L_LINENUMBER);";
    AlterExecute(context, client, stmt);
    printf("Alter table: %s%d, create index ORDER_LINE done\n", TablePrefix.c_str(), idx);
  }
  {
    string stmt = "drop index PART on " + table + ";";
    AlterExecute(context, client, stmt);
    printf("Alter table: %s%d, drop PART done\n", TablePrefix.c_str(), idx);
  }
  {
    string stmt = "drop index PART_ORDER on " + table + ";";
    AlterExecute(context, client, stmt);
    printf("Alter table: %s%d, drop PART_ORDER done\n", TablePrefix.c_str(), idx);
  }
  {
    string stmt = "create index PART on " + table + " (L_PARTKEY);";
    AlterExecute(context, client, stmt);
    printf("Alter table: %s%d, create index PART done\n", TablePrefix.c_str(), idx);
  }
  {
    string stmt = "create index PART_ORDER on " + table + "(L_PARTKEY, L_ORDERKEY);";
    AlterExecute(context, client, stmt);
    printf("Alter table: %s%d, create composite index PART_ORDER done\n", TablePrefix.c_str(), idx);
  }
  release_lock(table);
  printf("done Alter table: %s%d\n", TablePrefix.c_str(), idx);
}

void AlterExecute(Context& context, Mysql& client, const std::string& stmt) {
  MYSQL_STMT* m_stmt = client.prepare(stmt);
  client.execute(m_stmt);
  client.release_stmt(m_stmt);
}

/*
 * TBD:
 * 1. add back date fields
 * 2. to employ txn + bulk-insert
 */
void Insert(Context& context) {
  Mysql client;
  if (!client.connect()) {
    printf("Insert(): conn failed\n");
    return;
  }
  int idx = context.mt() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(context, idx);

  string str_stmt = "Insert into " + table +
    " values(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
  MYSQL_STMT* stmt = client.prepare(str_stmt);
  int row_start = context.mt() % contents.size();
  int limit = min<size_t>(row_cnt, contents.size() - row_start + 1);
  //printf("Insert table: %s%d, cnt %d\n", TablePrefix.c_str(), idx, limit);
  for (int cnt = 0; cnt < row_cnt; cnt++) {
    if (row_start + cnt >= contents.size())
      break;

    const string& line = contents[cnt + row_start];
    std::vector<string> results;
    boost::split(results, line, [](char c){return c == '|';});

    MYSQL_BIND in_params[17];
    memset(in_params, 0, sizeof(in_params));

    client.bind_arg(in_params[0], atoi(results[kOrderKey].c_str()));
    client.bind_arg(in_params[1], atoi(results[kPartKey].c_str()));

    client.bind_arg(in_params[2], atoi(results[kSuppKey].c_str()));
    client.bind_arg(in_params[3], atoi(results[kLineNumber].c_str()));

    client.bind_arg(in_params[4], atof(results[kQuantity].c_str()));
    client.bind_arg(in_params[5], atof(results[kExtendedPrice].c_str()));
    client.bind_arg(in_params[6], atof(results[kDiscount].c_str()));
    client.bind_arg(in_params[7], atof(results[kTax].c_str()));

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
  printf("done Insert table: %s%d\n", TablePrefix.c_str(), idx);
}

void InsertBulk(Context& context, Mysql& client, int table_idx, int offset, int round_cnt) {
  string table = TablePrefix + to_string(table_idx);
  for (int i = 0; i < round_cnt; i++) {
    if (offset + i >= contents.size())
      continue;
    const string& line = contents[offset + i];
    std::vector<string> results;
    boost::split(results, line, [](char c){return c == '|';});

    stringstream sst;
    sst << "Insert into " << table << " values("
        << results[kOrderKey] << ", "
        << results[kPartKey] << ", "
        << results[kSuppKey] << ", "
        << results[kLineNumber] << ", "
        << results[kQuantity] << ", "
        << results[kExtendedPrice] << ", "
        << results[kDiscount] << ", "
        << results[kTax] << ", "
        << "'"  << results[kReturnFlag]   << "'"  << ", "
        << "'"  << results[kLineStatus]   << "'"  << ", "
        << "\"" << results[kShipDate] << "\"" << ", "
              << "\"" << results[kCommitDate] << "\"" << ", "
        << "\"" << results[kRecepitDate] << "\"" << ", "
        << "\"" << results[kShipinStruct] << "\"" << ", "
        << "\"" << results[kShipMode]     << "\"" << ", "
        << "\"" << results[kComment]      << "\"" << ")";

    client.execute(sst.str());
    total_counts += 1;
  }
  //printf("done Insert table: %s%d\n", TablePrefix.c_str(), table_idx);
}

void Update(Context& context, Mysql& client, int table_idx, int offset, int round_cnt) {
  string table = TablePrefix + to_string(table_idx);
  for (int i = 0; i < round_cnt; i++) {
    if (offset + i >= contents.size())
      continue;
    const string& line = contents[offset + i];
    std::vector<string> results;
    boost::split(results, line, [](char c){return c == '|';});

    stringstream sst;
    int sup = atoi(results[kSuppKey].c_str()) * (offset - round_cnt) + round_cnt;
    int lin = atoi(results[kLineNumber].c_str()) * offset - round_cnt;
    string comm = results[kComment];
    if (comm.size() > 100)
      comm = comm.substr(1);
    sst << "Update " << table << " set "
        << " L_SUPPKEY=" << to_string(sup) << ", "
        << " L_LINENUMBER=" << to_string(lin) << ", "
        << " L_COMMENT=" << "\"" << comm      << "\"" << " "
        << "where L_ORDERKEY = " << results[kOrderKey] << " and "
        << "L_PARTKEY = " << results[kPartKey];

    client.execute(sst.str());
    total_counts += 1;
  }
}


void Delete(Context& context) {
  Mysql client;
  if (!client.connect()) {
    printf("Delete(): conn failed\n");
    return;
  }
  int idx = context.mt() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(context, idx);
  {
    string str_stmt = "delete from " + table +
      " where L_ORDERKEY = ? and L_PARTKEY = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(context, client, stmt, kOrderKey, kPartKey);
  }
  {
    string str_stmt = "delete from " + table +
      " where id = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(context, client, stmt, -1, -1);
  }
  release_lock(table);
  printf("done Delete: %s%d\n", TablePrefix.c_str(), idx);
}

// query with primary key? or secondary key
void Query(Context& context) {
  Mysql client;
  if (!client.connect()) {
    printf("Query(): conn failed\n");
    return;
  }
  int idx = context.mt() % table_cnt;
  string table = TablePrefix + to_string(idx);
  if (!try_lock(table))
    return;
  CreateTable(context, idx);

  {
    string str_stmt = "select * from " + table +
      " where L_ORDERKEY = ? and L_PARTKEY = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(context, client, stmt, kOrderKey, kPartKey);
  }
  {
    string str_stmt = "select * from " + table +
      " where L_SUPPKEY = ? and L_PARTKEY = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(context, client, stmt, kSuppKey, kPartKey);
  }
  {
    string str_stmt = "select * from " + table +
      " where id = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(context, client, stmt, -1, -1);
  }
  {
    string str_stmt = "select * from " + table +
      " where L_ORDERKEY = ?";
    MYSQL_STMT* stmt = client.prepare(str_stmt);
    QueryExecutePrepared(context, client, stmt, kOrderKey, -1);
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
void QueryExecute(Context& context, Mysql& client, const std::string& str_in, int idx1, int idx2) {
  int row_start = context.mt() % contents.size();
  int limit = min<size_t>(row_cnt, contents.size() - row_start + 1);
  for (int cnt = 0; cnt < row_cnt; cnt++) {
    if (row_start + cnt >= contents.size())
      break;
    const string& line = contents[cnt + row_start];
    std::vector<string> results;
    boost::split(results, line, [](char c){return c == '|';});
    string str_stmt = str_in;
    if (idx1 != -1 && idx2 != -1) {
      replace_with(str_stmt, "?", results[idx1]);
      replace_with(str_stmt, "?", results[idx2]);
      client.execute(str_stmt);
    } else if (idx1 != -1) {
      replace_with(str_stmt, "?", results[idx1]);
      client.execute(str_stmt);
    }
    client.consume_data(results);
  }
}

void QueryExecutePrepared(Context& context, Mysql& client, MYSQL_STMT* stmt, int idx1, int idx2) {
  int row_start = context.mt() % contents.size();
  int limit = min<size_t>(row_cnt, contents.size() - row_start + 1);
  //printf("table: stmt %s, cnt %d\n", str.c_str(), limit);
  for (int cnt = 0; cnt < row_cnt; cnt++) {
    if (row_start + cnt >= contents.size())
      break;
    const string& line = contents[cnt + row_start];
    std::vector<string> results;
    boost::split(results, line, [](char c){return c == '|';});
    if (idx1 != -1 && idx2 != -1) {
      MYSQL_BIND in_params[2];
      client.bind_arg(in_params[0], atoi(results[idx1].c_str()));
      client.bind_arg(in_params[1], atoi(results[idx2].c_str()));
      client.bind_execute(stmt, in_params);
    } else if (idx1 != -1) {
      MYSQL_BIND in_params[1];
      client.bind_arg(in_params[0], atoi(results[idx1].c_str()));
      client.bind_execute(stmt, in_params);
    } else {
      MYSQL_BIND in_params[1];
      client.bind_arg(in_params[0], cnt + row_start);
      client.bind_execute(stmt, in_params);
    }
    client.consume_data(stmt);
  }
}

MYSQL_RES* QueryExecuteAndReturn(Context& context, Mysql& client, const std::string& str_in, 
                                 int offset, int idx1, int idx2) {
  const string& line = contents[offset];
  std::vector<string> results;
  boost::split(results, line, [](char c){return c == '|';});
  string str_stmt = str_in;
  if (idx1 != -1 && idx2 != -1) {
    replace_with(str_stmt, "?", results[idx1]);
    replace_with(str_stmt, "?", results[idx2]);
    client.execute(str_stmt);
  } else if (idx1 != -1) {
    replace_with(str_stmt, "?", results[idx1]);
    client.execute(str_stmt);
  }
  return client.store_result();
}

