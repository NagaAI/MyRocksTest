
#include <time.h>
#include <functional>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include "client.h"
#include "run_test.h"

using namespace std;

typedef function<void()> executor;
vector<executor> executors;
//map<string, string
vector<string> contents;

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
  string path = "./lineitem.tbl";
  ifstream in(path.c_str());
  string line;
  while (getline(in, line)) {
    if (line.size() < 10)
      continue;
    contents.push_back(line);
  }
}

bool connect(Mysql& client) {
  if (!client.connect()) {
    printf("conn failed\n");
    return false;
  }
  printf("conn succ\n");
  return true;
}

void generate_executor() {
  srand (time(NULL));
  int type = rand() % 7;
}

void CreateTable() {
  int idx = rand() % kTableLimit;
  stringstream ss;
  ss << "(L_ORDERKEY    BIGINT NOT NULL, "
     << "L_PARTKEY     BIGINT NOT NULL,"
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
     << "PRIMARY KEY (L_ORDERKEY, L_PARTKEY),"
     << "KEY (L_ORDERKEY),"
     << "KEY (L_ORDERKEY, L_SUPPKEY),"
     << "KEY (L_PARTKEY),"
     << "KEY (L_PARTKEY, L_ORDERKEY),"
     << "KEY (L_PARTKEY, L_SUPPKEY),"
     << "KEY (L_SUPPKEY),"
     << "KEY (L_SUPPKEY, L_ORDERKEY),"
     << "KEY (L_SUPPKEY, L_PARTKEY));";
  string stmt = "create table " + TablePrefix +
    to_string(idx) + ss.str();
  Mysql client;
  if (!client.connect()) {
    printf("conn failed\n");
    return;
  }
  MYSQL_STMT* m_stmt = client.prepare(stmt);
  client.execute(m_stmt);
}

void DropTable() {
  int idx = rand() % kTableLimit;
  string stmt = "drop table " + TablePrefix + to_string(idx);
  Mysql client;
  if (!client.connect()) {
    printf("conn failed\n");
    return;
  }
  MYSQL_STMT* m_stmt = client.prepare(stmt);
  client.execute(m_stmt);
}

/*
 * TBD:
 * 1. add back date fields
 * 2. to employ txn
 */
void Insert() {
  Mysql client;
  if (!client.connect()) {
    printf("conn failed\n");
    return;
  }
  //int idx = rand() % kTableLimit;
  int idx = 3;
  string str_stmt = "Insert into " + TablePrefix + to_string(idx) +
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
}

void Delete() {
  
}

