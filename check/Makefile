.PHONY: spellcheck test upload

PWD=`pwd`

all: reeder check

reeder:
	javac -classpath /.freespace/$(USER)/hadoop/lib/commons-logging-1.1.1.jar:/.freespace/$(USER)/hadoop/hadoop-core-0.20.203.0.jar:/.freespace/$(USER)/hadoop/lib/commons-cli-1.2.jar reeder.java -target 1.6 -source 1.6

check: reeder
	java -classpath /.freespace/$(USER)/hadoop/lib/commons-logging-1.1.1.jar:/.freespace/$(USER)/hadoop/hadoop-core-0.20.203.0.jar:/.freespace/$(USER)/hadoop/lib/commons-cli-1.2.jar:/.freespace/$(USER)/hadoop/lib/*:. reeder part-00000

