# Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

build:
	cd ../../networking; make
	g++ -std=c++11 -pedantic -Wall -pthread -g wc_node.cpp -o wc_node
	g++ -std=c++11 -pedantic -Wall -pthread -g word_count.cpp -o word_count

run:
	# runs word_count with small.txt file and 3 counters
	./word_count small.txt 1

valgrind:
	valgrind --trace-children=yes --leak-check=full ./word_count small.txt 1

clean:
	rm -f word_count
	rm -f wc_node
	rm -f ../../networking/server
