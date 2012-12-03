HadoopSpellCheck
================

Check spelling on large data sets w/ hadoop. 

To run the WordCount.java program: 
```
cd ~$USER/HadoopSpellCheck
javac -classpath /.freespace/$USER/hadoop/lib/commons-logging-1.1.1.jar:/.freespace/$USER/hadoop/hadoop-core-0.20.203.0.jar:/.freespace/$USER/hadoop/lib/commons-cli-1.2.jar  -d wordcount WordCount.java -target 1.6 -source 1.6
cd /.freespace/$USER/hadoop
jar cvf wordcount.jar -C ~$USER/HadoopSpellCheck/wordcount .
 bin/hadoop jar wordcount.jar WordCount -Dmapred.reduce.tasks=10 /tmp/hadoop/$USER/input//enwiki-20080103.user_talk /tmp/hadoop/$USER/output
```
