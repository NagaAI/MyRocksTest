

CXX=g++
CXXFLAGS=-g3 -std=c++11 -O2
INCLUDE=-I. -I/usr/include/mysql -I/disk/terark-home/Programs/boost_1_62_0 -I/newssd1/zzz/terark/src
LIB=-L/newssd1/temp/mysql-on-terarkdb-4.8-bmi2-0/lib -L/usr/lib64/mysql
OBJS=run_test.o client.o

all: ${OBJS}
	${CXX} ${CXXFLAGS} ${INCLUDE} ${LIB} -o main main.cc ${OBJS} -lmysqlclient -lpthread

run_test.o:
	${CXX} ${CXXFLAGS} ${INCLUDE} -c run_test.cc

client.o:
	${CXX} ${CXXFLAGS} ${INCLUDE} -c client.cc

clean:
	rm *.o main
