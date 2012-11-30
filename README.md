HadoopSpellCheck
================

Check spelling on large data sets w/ hadoop. 

To run the WordCount.java program: 
```
cd ~jarek/HadoopSpellCheck
javac -classpath /.freespace/jarek/hadoop/hadoop-core-0.20.203.0.jar:/.freespace/jarek/hadoop/lib/commons-cli-1.2.jar  -d wordcount WordCount.java
cd /.freespace/jarek/hadoop
jar cvf wordcount.jar -C ~jarek/HadoopSpellCheck/wordcount .
 bin/hadoop jar wordcount.jar WordCount /tmp/hadoop/jarek/input//enwiki-20080103.user_talk /tmp/hadoop/jarek/output
```
