//
// Created by wangfo on 17-7-21.
//

#ifndef MEDIAWIKI_DATABASE_BENCHMARK_MYSQL_H
#define MEDIAWIKI_DATABASE_BENCHMARK_MYSQL_H

#include "mysql.h"
#include <cstdio>
#include <string>
#include <cstring>
#include <errmsg.h>
#include <vector>

class Mysql
{
public:
  Mysql() {
    //connect();
  }
  Mysql(const char *_host, const char *_user, const char *_passwd, const char *_db, const unsigned int &_port)
      : host(_host), user(_user), passwd(_passwd), db(_db), port(_port) {
    connect();
  }
	  //Mysql(const char *_host, const char *_user, const char *_passwd, const char *_db) :
          //Mysql(_host, _user, _passwd, _db, 3306) { }

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
  MYSQL_RES* use_result();
  void       free_result(MYSQL_RES*);
  void       consume_data(MYSQL_STMT*);
  std::string str = R"foo(123)foo";
  
private:
  const char *host = "127.0.0.1";
  const char *user = "root";
  const char *passwd = "";
  const char *db = "tpch";
  //const unsigned int port = 3336;   // 120G data, innodb, QPS
  unsigned int port = 3307; // 120G data, Terark, QPS
  //const unsigned int port = 3316;  // no data, Terark, CRUD test

  MYSQL *conn_;
  MYSQL_STMT *get_one_stmt;

  MYSQL_BIND get_one_out_params[5];
  MYSQL_BIND get_one_in_params[1];


};

#endif //MEDIAWIKI_DATABASE_BENCHMARK_MYSQL_H
