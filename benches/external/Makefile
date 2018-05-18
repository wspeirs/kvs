GPP   = /usr/bin/g++
GCC   = /usr/bin/gcc
FLAGS = -std=c++11 -W -Wall -g # -O3
LIBS  = libleveldb_v1.20.a librocksdb_v5.12.4.a -lpthread
INC   = -I.
EXEC  = benchmarks

all: main.o 
	$(GPP) $(FLAGS) *.o $(LIBS) -o $(EXEC)

main.o: main.cpp
	$(GPP) -c $(FLAGS) $(INC) main.cpp

clean:
	rm *~ *.o $(EXEC)
