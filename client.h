//
// Created by wangfo on 17-7-21.
//

#ifndef MEDIAWIKI_DATABASE_BENCHMARK_MYSQL_H
#define MEDIAWIKI_DATABASE_BENCHMARK_MYSQL_H

#include <assert.h>
#include <cstdio>
#include <string>
#include <cstring>
#include <errmsg.h>
#include <vector>

#include "mysql.h"

class Mysql
{
public:
  Mysql() {
    host_ = "127.0.0.1";
    db_ = "tpch";
    //
    char* cport = getenv("port");
    if (cport)
      port_ = atoi(cport);
    assert(port_ < 70000);
  }

 Mysql(const std::string& port_name) {
    host_ = "127.0.0.1";
    db_ = "tpch";
    char* cport = getenv(port_name.c_str());
    assert(cport != nullptr);
    port_ = atoi(cport);
    assert(port_ < 70000);
 }
  /*Mysql(const char *_host, const char *_user, const char *_passwd, const char *_db, const unsigned int &_port)
      : host(_host), user(_user), passwd(_passwd), db(_db), port(_port) {
    connect();
    }*/

  ~Mysql() {
    mysql_close(conn_);
  }

  //virtual bool get_one(ThreadState *state);
//  virtual bool update_one();
//  virtual bool insert_one();
//  virtual bool delete_one();
//  virtual bool exec_one_op();
  virtual bool get_all_keys(std::vector<int> *vec);

  bool connect();

  static void verify_data(MYSQL_RES* res, MYSQL_RES* ref_res);

public:
  MYSQL_STMT *prepare(std::string query);
  bool release_stmt(MYSQL_STMT* stmt);
  void bind_arg(MYSQL_BIND &b, const int &val);
  void bind_arg(MYSQL_BIND &b, const double &val);
  void bind_arg(MYSQL_BIND &b, const char *val, size_t length);
  void execute(MYSQL_STMT *stmt);
  void execute(const std::string& str_stmt);
  void bind_execute(MYSQL_STMT *stmt, MYSQL_BIND *params);
  /*
   * Commands out of sync; you can't run this command now.
   * will happen if you try to execute two queries that return data without 
   * calling mysql_use_result() or mysql_store_result() in between.
   */
  MYSQL_RES* store_result();
  MYSQL_RES* use_result();
  void       free_result(MYSQL_RES*);
  void       consume_data(MYSQL_STMT*);
  void       consume_data(std::vector<std::string>&);
  std::string str = R"foo(123)foo";
  
private:
  std::string host_;
  const char *user = "root";
  const char *passwd = "";
  std::string db_;
  //const unsigned int port = 3336;   // 120G data, innodb, QPS
  unsigned int port_ = 3307; // 120G data, Terark, QPS
  //const unsigned int port = 3316;  // no data, Terark, CRUD test

  MYSQL *conn_;
  MYSQL_STMT *get_one_stmt;

  MYSQL_BIND get_one_out_params[5];
  MYSQL_BIND get_one_in_params[1];


};

#endif //MEDIAWIKI_DATABASE_BENCHMARK_MYSQL_H
