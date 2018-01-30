//
// Created by wangfo on 17-7-21.
//

#include <assert.h>
#include <stdlib.h>
//#include <terark/fstring.hpp>
#include "client.h"


bool Mysql::connect() {
  {
    char* phost = getenv("host");
    if (phost)
      host_ = phost;
    assert(!host_.empty());
  }
  {
    char* pdb = getenv("db");
    if (pdb)
      db_ = pdb;
    assert(!db_.empty());
  }

  conn_ = mysql_init(NULL);
  unsigned long client_flag = CLIENT_REMEMBER_OPTIONS;
  if(!mysql_real_connect(conn_, host_.c_str(), user, passwd, db_.c_str(), port_, NULL, client_flag)) {
    fprintf(stderr,
            "ERROR: mysql_real_connect(host=%s, user=%s, passwd=%s, db=%s, port=%d, NULL, CLIENT_REMEMBER_OPTIONS) = %s\n"
            "database connection fails.\n", host_.c_str(), user, passwd, db_.c_str(), port_, mysql_error(conn_));
    return false;
  }

  //fprintf(stderr, "database connected!\n");
  return true;
}

// prepared statement
MYSQL_STMT* Mysql::prepare(std::string query) {
  MYSQL_STMT *stmt = mysql_stmt_init(conn_);
  int err = mysql_stmt_prepare(stmt, query.c_str(), query.size());
  if (err) {
    fprintf(stderr, "ERROR: mysql_stmt_prepare(%s) = %s \n", query.c_str(), mysql_error(conn_));
    //mysql_stmt_close(stmt);
    //exit(1);
  }
  return stmt;
}

bool Mysql::release_stmt(MYSQL_STMT* stmt) {
  assert(stmt != nullptr);
  int err = mysql_stmt_close(stmt);
  if (err) {
    fprintf(stderr, "ERROR: mysql_stmt_close() = %s \n", mysql_error(conn_));
    return false;
  }
  return true;
}

void Mysql::bind_arg(MYSQL_BIND &b, const int &val) {
  memset(&b, 0, sizeof(b));
  b.buffer_length = 4;
  b.buffer_type = MYSQL_TYPE_LONG;
  b.buffer = (void *)&val;
}

void Mysql::bind_arg(MYSQL_BIND &b, const double &val) {
  memset(&b, 0, sizeof(b));
  b.buffer_length = 8;
  b.buffer_type = MYSQL_TYPE_DOUBLE;
  b.buffer = (void *)&val;
}

void Mysql::bind_arg(MYSQL_BIND &b, const char *val, size_t length) {
  memset(&b, 0, sizeof(b));
  b.buffer_length = length;
  b.buffer_type =  MYSQL_TYPE_STRING;
  b.buffer = (void *)val;
}

void Mysql::execute(const std::string& str_stmt) {
  if (mysql_query(conn_, str_stmt.c_str())) {
    fprintf(stderr, "ERROR: mysql stmt execute = %s\n", str_stmt.c_str());
    //exit(1);
  }
}

void Mysql::execute(MYSQL_STMT *stmt) {
  if (mysql_stmt_execute(stmt)) {
    fprintf(stderr, "ERROR: mysql stmt execute = %s\n", mysql_stmt_error(stmt));
    //exit(1);
  }
  //if (mysql_stmt_store_result(stmt)) {
  //  fprintf(stderr, "ERROR: mysql stmt store result = %s\n", mysql_stmt_error(stmt));
    //exit(1);
  //}
}
void Mysql::bind_execute(MYSQL_STMT *stmt, MYSQL_BIND *params) {
  if (mysql_stmt_bind_param(stmt, params)) {
    fprintf(stderr, "ERROR: mysql stmt bind = %s\n", mysql_stmt_error(stmt));
    exit(1);
  }
  execute(stmt);
}

void Mysql::consume_data(MYSQL_STMT* stmt) {
  assert(stmt != nullptr);
  while (!mysql_stmt_fetch(stmt))
    ;
}

void Mysql::verify_data(MYSQL_RES* res, MYSQL_RES* ref_res) {
  assert(res && ref_res);
  assert(mysql_num_fields(res) == mysql_num_fields(ref_res));
  unsigned int num_fields = mysql_num_fields(res);
  MYSQL_ROW row = mysql_fetch_row(res),
    ref_row = mysql_fetch_row(ref_res);
  assert(ref_row != NULL && row != NULL);
  { // assume one row
    unsigned long *ref_lengths = mysql_fetch_lengths(ref_res);
    unsigned long *lengths = mysql_fetch_lengths(res);
    for (int i = 0; i < num_fields; i++) {
      std::string str(row[i], lengths[i]);
      std::string ref_str(ref_row[i], ref_lengths[i]);
      assert(str == ref_str);
      /*if (ref_str != str) {
	printf("expected: [%s], actual [%s] ",
	       ref_str.c_str(), str.c_str());
	       }*/
    }
  }
}

void Mysql::consume_data(std::vector<std::string>& inputs) {
  MYSQL_RES* result = mysql_store_result(conn_);
  /*MYSQL_ROW row;
  unsigned int num_fields = mysql_num_fields(result);
  while ( (row = mysql_fetch_row(result)) != NULL ) {
    unsigned long *lengths;
    lengths = mysql_fetch_lengths(result);
    for (int i = 0; i < num_fields; i++) {
      std::string str(row[i], lengths[i]);
      if (row[i] != str) {
	printf("expected: [%s], actual [%.*s] ", inputs[i].c_str(), 
	       (int) lengths[i], row[i] ? row[i] : "NULL");
      }
    }
    printf("\n");
    }*/
  /* free the result set */
  mysql_free_result(result);
}

MYSQL_RES* Mysql::store_result() {
  return mysql_store_result(conn_);
}

MYSQL_RES* Mysql::use_result() {
  return mysql_use_result(conn_);
}

void Mysql::free_result(MYSQL_RES* res) {
  //assert(res != nullptr);
  mysql_free_result(res);
}

bool Mysql::get_all_keys(std::vector<int> *vec)
{
  MYSQL_STMT *stmt = prepare("select page_id from page");

  int key;
  MYSQL_BIND params[1];
  bind_arg(params[0], key);

  bind_execute(stmt, params);

  int status = mysql_stmt_fetch(stmt);
  if (status == MYSQL_NO_DATA) {
    fprintf(stderr, "ERROR: there are no data to fetch\n");
  } else if (status == MYSQL_DATA_TRUNCATED) {
    fprintf(stderr, "ERROR: data truncated\n");
  }
  while (!status) {
    //printf("%s\n", key);
    vec->push_back(key);
    status = mysql_stmt_fetch(stmt);
  }

  mysql_stmt_close(stmt);
  return true;
}

