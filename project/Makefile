# Authors: Zoe Corning(corning.z@husky.neu.edu) & Rucha Khanolkar(khanolkar.r@husky.neu.edu)

# builds and runs all unit tests
build:
	# serialization tests
	(cd networking/serialization/tests; make) > /dev/null
	# networking tests
	(cd networking/tests; make) > /dev/null
	# dataframe tests
	(cd data/dataframe/tests; cmake .; make) > /dev/null
	# kv_store tests
	(cd data/kv_store/tests; make) > /dev/null
	# sorer tests
	(cd data/sorer/tests; make) > /dev/null
	# demo
	(cd application/demo; make) > /dev/null
	# word_count
	(cd application/word_count; make) > /dev/null
	# linus
	(cd application/linus; make) > /dev/null
	# SUCCESS - ALL TESTS COMPILED

run:
	# serialization tests
	(cd networking/serialization/tests; make run) > /dev/null
	# networking tests
	(cd networking/tests; make run) > /dev/null
	# dataframe tests
	(cd data/dataframe/tests; ./run_all) > /dev/null
	# kv_store tests
	(cd data/kv_store/tests; make run) > /dev/null
	# sorer tests
	(cd data/sorer/tests; make run) > /dev/null
	# demo
	(cd application/demo; make run) > /dev/null
	# word_count
	(cd application/word_count; make run) > /dev/null
	# SUCCESS - ALL TESTS PASSED

run_linus:
	# linus
	cd application/linus; make time

valgrind:
	# serialization
	cd networking/serialization/tests; make valgrind
	# networking
	cd networking/tests; make valgrind
	# kv_store
	cd data/kv_store/tests; make valgrind
	# sorer
	cd data/sorer/tests; make valgrind
	# demo
	cd application/demo; make valgrind
	# word_count
	cd application/word_count; make
	# SUCCESS - NO MEMORY LEAKS

valgrind_linus:
	# linus
	cd application/linus; make valgrind

# cleans up all test folders after running tests
clean:
	# clean serialization folder
	(cd networking/serialization/tests; make clean) > /dev/null
	# clean networking folder
	(cd networking; make clean; cd tests; make clean) > /dev/null
	# clean dataframe test folder
	(cd data/dataframe/tests; ./rm_google_stuff) > /dev/null
	# clean kv_store test folder
	(cd data/kv_store/tests; make clean) > /dev/null
	# clean sorer test folder
	(cd data/sorer/tests; make clean) > /dev/null
	# clean demo folder
	(cd application/demo; make clean) > /dev/null
	# clean word_count folder
	(cd application/word_count; make clean) > /dev/null
	# clean linus folder
	(cd application/linus; make clean) > /dev/null
	# ALL CLEAN
