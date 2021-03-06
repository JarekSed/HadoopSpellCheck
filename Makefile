.PHONY: spellcheck test upload

PWD=`pwd`
HADOOP=/.freespace/$(USER)/hadoop/bin/hadoop
OUTJAR=/.freespace/$(USER)/hadoop/spellcheck.jar

all: spellcheck

spellcheck:
	mkdir -p spellcheck
	javac -classpath /.freespace/$(USER)/hadoop/lib/commons-logging-1.1.1.jar:/.freespace/$(USER)/hadoop/hadoop-core-0.20.203.0.jar:/.freespace/$(USER)/hadoop/lib/commons-cli-1.2.jar  -d spellcheck SpellCheck.java -target 1.6 -source 1.6
	jar cvf $(OUTJAR) -C $(PWD)/spellcheck .

upload:
	$(HADOOP) dfs -put $(PWD)/big.txt /tmp/rjy/big.txt; exit 0
	$(HADOOP) dfs -put $(PWD)/simple.txt /tmp/hadoop/$(USER)/input/simple.txt; exit 0

test: spellcheck
	$(HADOOP) fs -rmr /tmp/hadoop/$(USER)/output; exit 0
	$(HADOOP) jar $(OUTJAR) SpellCheck -Dmapred.reduce.tasks=10 /tmp/hadoop/$(USER)/input/stock_symbol_keywords.tsv /tmp/hadoop/$(USER)/output

big_test: spellcheck upload
	$(HADOOP) fs -rmr /tmp/hadoop/$(USER)/output; exit 0
	$(HADOOP) jar $(OUTJAR) SpellCheck -Dmapred.reduce.tasks=50  -Dmapred.map.tasks=50 -Dmapred.tasktracker.reduce.tasks.maximum=50 -Dmapred.task.timeout=18000000  /tmp/hadoop/jarek/input/enwiki-20080103.user_talk /tmp/hadoop/$(USER)/output

simple_test: spellcheck
	$(HADOOP) fs -rmr /tmp/hadoop/$(USER)/output; exit 0
	$(HADOOP) jar $(OUTJAR) SpellCheck /tmp/hadoop/$(USER)/input/simple.txt /tmp/hadoop/$(USER)/output
	make download

# download results
download:
	rm -rf output
	mkdir -p output
	$(HADOOP) fs -get /tmp/hadoop/$(USER)/output/* output/
