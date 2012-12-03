.PHONY: wordcount

PWD=`pwd`

all: wordcount

wordcount:
	javac -classpath /.freespace/$(USER)/hadoop/lib/commons-logging-1.1.1.jar:/.freespace/$(USER)/hadoop/hadoop-core-0.20.203.0.jar:/.freespace/$(USER)/hadoop/lib/commons-cli-1.2.jar  -d wordcount WordCount.java -target 1.6 -source 1.6
	jar cvf /.freespace/$(USER)/hadoop/wordcount.jar -C $(PWD)/wordcount .
