.PHONY: spellcheck

PWD=`pwd`
HADOOP=/.freespace/$(USER)/hadoop/bin/hadoop

all: spellcheck

spellcheck:
	javac -classpath /.freespace/$(USER)/hadoop/lib/commons-logging-1.1.1.jar:/.freespace/$(USER)/hadoop/hadoop-core-0.20.203.0.jar:/.freespace/$(USER)/hadoop/lib/commons-cli-1.2.jar  -d spellcheck SpellCheck.java -target 1.6 -source 1.6
	jar cvf /.freespace/$(USER)/hadoop/spellcheck.jar -C $(PWD)/spellcheck .

upload:
	$(HADOOP) dfs -put $(PWD)/big.txt /tmp/rjy/big.txt
