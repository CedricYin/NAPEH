.PHONY: all clean

CXX := g++ -O3 -std=c++17
LDDINC := $(shell pwd)
INC_CLI := $(LDDINC)
INC_PUB  := $(LDDINC)/../include
INC_NAPEH  := $(LDDINC)/../include/napeh
CFLAGS := -I./ -lrt
CFLAGS   += -I$(INC_CLI) -I$(INC_PUB) -I$(INC_NAPEH) -lrt

all: test_NAPEH

test_NAPEH: test_NAPEH.cpp 
	$(CXX) $(CFLAGS) -o test_NAPEH   test_NAPEH.cpp  -mclwb -lpthread -lnuma -lstdc++fs -g 
	
clean:
	rm -rf *.o ./test_NAPEH
