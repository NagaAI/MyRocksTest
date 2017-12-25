//
// Created by wangfo on 17-7-21.
//

#include "client.h"


bool Mysql::connect()
{
  //mysql_init(g_conn);
  g_conn = mysql_init(NULL);
  //my_bool myTrue = true;
  //mysql_options(conn, MYSQL_OPT_RECONNECT, &myTrue);
  unsigned long client_flag = CLIENT_REMEMBER_OPTIONS;
  if(!mysql_real_connect(g_conn, host, user, passwd, db, port, NULL, client_flag)) {
    fprintf(stderr,
            "ERROR: mysql_real_connect(host=%s, user=%s, passwd=%s, db=%s, port=%d, NULL, CLIENT_REMEMBER_OPTIONS) = %s\n"
            "database connection fails.\n", host, user, passwd, db, port, mysql_error(g_conn));
    return false;
  }

  //fprintf(stderr, "database connected!\n");
  return true;
}

// prepared statement
MYSQL_STMT* Mysql::prepare(std::string query) {
  MYSQL_STMT *stmt = mysql_stmt_init(g_conn);
  int err = mysql_stmt_prepare(stmt, query.c_str(), query.size());
  if (err) {
    fprintf(stderr, "ERROR: mysql_stmt_prepare(%s) = %s \n", query.c_str(), mysql_error(g_conn));
    mysql_stmt_close(stmt);
    exit(1);
  }
  return stmt;
}

void Mysql::bind_arg(MYSQL_BIND &b, const int &val) {
  memset(&b, 0, sizeof(b));
  b.buffer_length = 4;
  b.buffer_type = MYSQL_TYPE_LONG;
  b.buffer = (void *)&val;
}

void Mysql::bind_arg(MYSQL_BIND &b, const double &val) {
  memset(&b, 0, sizeof(b));
  b.buffer_length = 4;
  b.buffer_type = MYSQL_TYPE_DOUBLE;
  b.buffer = (void *)&val;
}

void Mysql::bind_arg(MYSQL_BIND &b, char *val, size_t length) {
  memset(&b, 0, sizeof(b));
  b.buffer_length = length;
  b.buffer_type =  MYSQL_TYPE_STRING;
  b.buffer = (void *)val;
}

void Mysql::execute(MYSQL_STMT *stmt) {
  if (mysql_stmt_execute(stmt)) {
    fprintf(stderr, "ERROR: mysql stmt execute = %s\n", mysql_stmt_error(stmt));
    //exit(1);
  }
  if (mysql_stmt_store_result(stmt)) {
    fprintf(stderr, "ERROR: mysql stmt store result = %s\n", mysql_stmt_error(stmt));
    //exit(1);
  }
}
void Mysql::bind_execute(MYSQL_STMT *stmt, MYSQL_BIND *params) {
  if (mysql_stmt_bind_result(stmt, params)) {
    fprintf(stderr, "ERROR: mysql stmt bind = %s\n", mysql_stmt_error(stmt));
    exit(1);
  }
  execute(stmt);
}

/*bool Mysql::get_one(ThreadState *state)
{
  //bind in params: page_id
  //MYSQL_BIND in_params[1];
  //int key_page_id = atoi(state->key.c_str());
  bind_arg(get_one_in_params[0], state->key);
  if (mysql_stmt_bind_param(get_one_stmt, get_one_in_params)) {
    fprintf(stderr, "ERROR: mysql stmt bind params = %s\n", mysql_stmt_error(get_one_stmt));
    exit(1);
  }
  bind_execute_store(get_one_stmt, get_one_out_params);

  int status = mysql_stmt_fetch(get_one_stmt);
  if (status == MYSQL_NO_DATA) {
    fprintf(stderr, "ERROR: there are no data to fetch\n");
  } else if (status == MYSQL_DATA_TRUNCATED) {
    fprintf(stderr, "ERROR: data truncated\n");
  }
*/
//  while (!status) {
//    printf("page_id: %d\npage_title: %s\nrevision_id: %d\ntext_id: %d\ntext: %s\n",
//          page_id, page_title, revision_id, text_id, "...text..."/*text*/);
//    status = mysql_stmt_fetch(get_one_stmt);
//  }

//return true;
//}

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
