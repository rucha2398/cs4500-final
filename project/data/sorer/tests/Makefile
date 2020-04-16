build: 
	g++ -o test -pedantic -Wall -std=c++11 -g test.cpp

run:
	./test

valgrind:
	valgrind --leak-check=full ./test

clean:
	rm -f test

.PHONY: test
