package solution2a;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LinuxMessageLogEmailMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
  public static IntWritable one = new IntWritable(1); 
  private boolean caseSensitive; 
  
  public void setup(Context context){
	  Configuration conf = context.getConfiguration();
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String type = null; 

    for (String email : line.split("\\W+")) {
      if (email.length() > 0) {
    	  context.write(new TextPair(email, type), one);
      }
    }
  }
}
