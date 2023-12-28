main: libnosql.a main.c nosql.o
	gcc -g main.c libnosql.a libev.a -o main

libnosql.a: nosql.o
	ar -rcs libnosql.a nosql.o

nosql.o: nosql.c nosql.h
	gcc -c -g nosql.c -o nosql.o
	
test:
	gcc -o test test.c libev.a

# -Wno-incompatible-pointer-types