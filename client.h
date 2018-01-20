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
    mysql_close(g_conn);
  }

  //virtual bool get_one(ThreadState *state);
//  virtual bool update_one();
//  virtual bool insert_one();
//  virtual bool delete_one();
//  virtual bool exec_one_op();
  virtual bool get_all_keys(std::vector<int> *vec);

  bool connect();
  
private:
  const char *host = "127.0.0.1";
  const char *user = "root";
  const char *passwd = "";
  const char *db = "tpch";
  //const unsigned int port = 3316;
  const unsigned int port = 3307;

  MYSQL *g_conn;
  const char *get_one_query = "select p.page_id as \"page_id\", "
          "p.page_title as \"page_title\", "
          "r.rev_text_id as \"revision_id\", "
          "t.old_id as \"text_id\", "
          "t.old_text as \"text\" from "
          "page p inner join revision r on p.page_latest = r.rev_id "
          "inner join text t on r.rev_text_id = t.old_id where p.page_id = ?";
  MYSQL_STMT *get_one_stmt;

  MYSQL_BIND get_one_out_params[5];
  MYSQL_BIND get_one_in_params[1];


public:
  MYSQL_STMT *prepare(std::string query);
  bool release_stmt(MYSQL_STMT* stmt);
  void bind_arg(MYSQL_BIND &b, const int &val);
  void bind_arg(MYSQL_BIND &b, const double &val);
  void bind_arg(MYSQL_BIND &b, const char *val, size_t length);
  void execute(MYSQL_STMT *stmt);
  void bind_execute(MYSQL_STMT *stmt, MYSQL_BIND *params);
  std::string str = R"foo(123)foo";
};

#endif //MEDIAWIKI_DATABASE_BENCHMARK_MYSQL_H
