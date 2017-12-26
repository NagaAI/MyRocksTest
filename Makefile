

CXX=g++
CXXFLAGS=-g -std=c++11
INCLUDE=-I. -I/usr/include/mysql -I/disk/terark-home/Programs/boost_1_62_0
OBJS=run_test.o client.o

all: ${OBJS}
	${CXX} ${CXXFLAGS} ${INCLUDE} -o main main.cc ${OBJS} -lmysqlclient

run_test.o:
	${CXX} ${CXXFLAGS} ${INCLUDE} -c run_test.cc

client.o:
	${CXX} ${CXXFLAGS} ${INCLUDE} -c client.cc

clean:
	rm *.o main
