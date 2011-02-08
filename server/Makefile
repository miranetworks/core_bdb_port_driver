SRCS=./src/kvs_bdb.c
OBJS=kvs_bdb.o
CFLAGS=
LDFLAGS=

all: clean kvs_bdb
$(OBJS):
	$(CC) $(CFLAGS) -O3 -c $(SRCS) 
kvs_bdb: $(OBJS)
	$(CC) $(LDFLAGS) -ldb -lm -lpthread -o ./bin/$@ $(OBJS)
	rm $(OBJS)
	cp ./bin/$@ ./scripts/usr/sbin/$@
clean:
	if [ -f ./bin/kvs_bdb ]; then rm ./bin/kvs_bdb; fi;
	if [ -f ./scripts/usr/sbin/kvs_bdb ]; then rm ./scripts/usr/sbin/kvs_bdb; fi;

