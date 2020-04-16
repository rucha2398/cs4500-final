# Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

build:
	cd ../../networking; make
	g++ -std=c++11 -pedantic -Wall -pthread -g demo_node.cpp -o demo_node
	g++ -std=c++11 -pedantic -Wall -pthread -g demo.cpp -o demo

run:
	./demo

valgrind:
	valgrind --trace-children=yes --leak-check=full ./demo

clean:
	rm -f demo_node
	rm -f demo
	rm -f ../../networking/server
