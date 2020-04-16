build:
	cd ../; make
	g++ -std=c++11 -pthread -Wall -pedantic -g node.cpp -o node
	g++ -std=c++11 -g -Wall -pedantic startup.cpp -o startup

run:
	./startup 4

valgrind:
	valgrind --trace-children=yes --leak-check=full ./startup 3

clean:
	rm -f startup; rm -f node
