import java.io.IOException;                                                                                                                                                                                                                                                     
import java.io.File;                                                               
import java.io.*;                                                                  
import java.util.regex.*;                                                          
import java.net.URI;                                                               
import java.util.*;                                                                
import org.apache.hadoop.fs.Path;                                                  
import org.apache.hadoop.conf.Configuration;                                       
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
import org.apache.hadoop.mapred.Partitioner;                                       
import org.apache.hadoop.fs.PathFilter;                                            
import org.apache.hadoop.mapred.lib.InputSampler;                                  
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;                         
import org.apache.hadoop.filecache.DistributedCache;                               
import org.apache.hadoop.io.RawComparator;                                         
import org.apache.hadoop.io.SequenceFile;                                          
import org.apache.hadoop.io.NullWritable;                                          
import org.apache.hadoop.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Writable;
import org.apache.commons.logging.Log;                                             
import org.apache.commons.logging.LogFactory;                                      

public class reeder {

  public static void main (String args[]) {
    try {

      Configuration config = new Configuration();
      Path path = new Path(args[0]);
      SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
      WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
      Writable value = (Writable) reader.getValueClass().newInstance();
      while (reader.next(key, value)) {
        System.out.println(key + ": " + value);
      }
      reader.close();

    } catch (Exception e) { e.printStackTrace(); }
  }

}
