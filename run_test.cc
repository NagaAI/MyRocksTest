
#include <time.h>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

#include "client.h"
#include "run_test.h"

using namespace std;

typedef function<void()> executor;
vector<executor> executors;

enum e_type_t : int {
  kCreateTable = 0,
  kDropTable,
  kAlterTable,
  kInsert,
  kDelete,
  kUpdate
};

const int kTableLimit = 10;
string TablePrefix = "terark_";


void generate_executor() {
  srand (time(NULL));
  int type = rand() % 6;
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
     << "L_SHIPDATE    DATE NOT NULL,"
     << "L_COMMITDATE  DATE NOT NULL,"
     << "L_RECEIPTDATE DATE NOT NULL,"
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
  client.connect();
  MYSQL_STMT* m_stmt = client.prepare(stmt);
  client.execute(m_stmt);
}

void DropTable() {
  int idx = rand() % kTableLimit;
  string stmt = "drop table " + TablePrefix + to_string(idx);
  Mysql client;
  client.connect();
  MYSQL_STMT* m_stmt = client.prepare(stmt);
  client.execute(m_stmt);
}

void Insert() {
  
}
void Delete() {
  
}

