
#ifndef RUNTEST_H
#define RUNTEST_H


extern size_t thread_cnt;
extern size_t table_limit;
extern size_t row_cnt;

extern void Init();
extern void StartStress();
extern void CreateTable(int);
extern void DropTable();
extern void AlterTable();
extern void Insert();
extern void Delete();
extern void Query();

#endif
