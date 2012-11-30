import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;


public class WordCount {

    // This maps the text by counting how many times each word occurs. 
    // On each word, emits a <word, 1> pair
    public static class TokenizerMapper 
        extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter
                    ) throws IOException {
                // Tokenize by lots of nonalphanumeric chars.
                // If we did value.ToString.split() we could use a regex instead of hardcoded delims,
                // but this would use a ton of space.
                StringTokenizer itr = new StringTokenizer(value.toString(), " \n\t.,;:(){}[]`);");
                while (itr.hasMoreTokens()) {
                    // Make sure framework knows we are making progress.
                    reporter.progress();
                    // We saw another instance of the enxt word
                    word.set(itr.nextToken());
                    output.collect(word, one);
                }
            }
        }

    // Sums up the counts for each key.
    public static class IntSumReducer 
        extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterator<IntWritable> values, 
                    OutputCollector<Text, IntWritable> output, Reporter reporter
                    ) throws IOException{
                int sum = 0;
                while (values.hasNext()) {
                    IntWritable val = values.next();
                    reporter.progress();
                    sum += val.get();
                }
                result.set(sum);
                output.collect(key, result);
            }
        }

    public static class SwitchMapper<K, V>                                                                                                                                                                                                                                                                                              
        extends MapReduceBase implements Mapper<K, V, V, K> {

            // Swap key and value order.
            public void map(K key, V val,
                    OutputCollector<V, K> output, Reporter reporter)
                throws IOException {
                    output.collect(val, key);
                }
        }


    public static class SwitchReducer<K, V>
        extends MapReduceBase implements Reducer<K, V, V, K> {

            // Swap key and value order.
            public void reduce(K key, Iterator<V> values,
                    OutputCollector<V, K> output, Reporter reporter)
                throws IOException {
                    while (values.hasNext()) {
                        output.collect(values.next(), key);
                    }                                                                                                                                                                                                                                                                    
                }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path tmp_path = new Path("/tmp/tmp_wordcount_sort"); 
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        JobConf jobConf1 = new JobConf(conf, WordCount.class);
        jobConf1.setJobName("Word Count");

        jobConf1.setMapperClass(TokenizerMapper.class);        
        jobConf1.setReducerClass(IntSumReducer.class);
        jobConf1.setCombinerClass(IntSumReducer.class);

        JobClient client1 = new JobClient(jobConf1);
        jobConf1.setOutputFormat(SequenceFileOutputFormat.class);
        jobConf1.setOutputKeyClass(Text.class);
        jobConf1.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(jobConf1, otherArgs[0]);
        FileOutputFormat.setOutputPath(jobConf1, tmp_path);

        Date start_time1 = new Date();
        System.out.println("Job started: " + start_time1);
        JobClient.runJob(jobConf1);
        Date end_time1 = new Date();
        System.out.println("Job ended: " + end_time1);
        System.out.println("The job took " + 
                (end_time1.getTime() - start_time1.getTime()) /1000 + " seconds.");



        /* Everything beneath here is for sorting the output.
         * if tmp_path is the output of the run above, and is a SequenceFile of Text keys
         * and IntWritable values, this run will sort them in decreasing order of value
         */
        Configuration newConf = new Configuration();
        JobConf jobConf = new JobConf(newConf, WordCount.class);
        tmp_path.getFileSystem(newConf).deleteOnExit(tmp_path);
        jobConf.setJobName("Word Count Sorted");

        jobConf.setMapperClass(SwitchMapper.class);        
        jobConf.setReducerClass(SwitchReducer.class);

        JobClient client = new JobClient(jobConf);
        jobConf.setInputFormat(SequenceFileInputFormat.class);
        jobConf.setOutputKeyClass(IntWritable.class);
        jobConf.setOutputValueClass(Text.class);
        // Sort in reverse order.
        jobConf.setKeyFieldComparatorOptions("-r");

        FileInputFormat.setInputPaths(jobConf, new Path(tmp_path, "part-00000"));
        FileOutputFormat.setOutputPath(jobConf, new Path(otherArgs[1]));

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        JobClient.runJob(jobConf);
        Date end_time = new Date();
        System.out.println("Job ended: " + end_time);
        System.out.println("The job took " + 
                (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");

    }
}
