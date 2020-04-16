# Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

build:
	g++ -std=c++11 -pedantic -Wall -pthread -g linus_node.cpp -o linus_node
	g++ -std=c++11 -pedantic -Wall -pthread -g linus.cpp -o linus
	cd ../../networking; make

run:
	# runs linus on big test files with 4 nodes
	./linus 4

time:
	# times a run of linus on big files with 5 nodes
	/usr/bin/time -v ./linus 5

valgrind:
	valgrind --trace-children=yes --leak-check=full ./linus 3

clean:
	rm -f linus_node
	rm -f linus
	rm -f ../../networking/server
