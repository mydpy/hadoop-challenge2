package solution2a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Challenge2a extends Configured implements Tool{

	  @Override
	  public int run(String[] args) throws Exception{
	    if (args.length != 2) {
	        System.err.printf("Usage: %s [generic options] <input> <output>\n",
	         	getClass().getSimpleName()); 
	        ToolRunner.printGenericCommandUsage(System.err);
	        return -1; 
	      }
	        
	    Configuration conf = getConf(); 
	    FileSystem fs = FileSystem.get(conf);
	    fs.delete(new Path(args[1]), true);
	    
	    Job job = Job.getInstance(conf, getClass().getSimpleName()); 
	    job.setJarByClass(getClass());
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(LinuxMessageLogEmailMapper.class); 
	    job.setReducerClass(SumReducer.class); 
	    
	    job.setOutputKeyClass(TextPair.class);
	    job.setOutputValueClass(IntWritable.class); 
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	  }
	  
	  public static void main(String[] args) throws Exception {
		  int exitCode = ToolRunner.run(new Configuration(), new Challenge2a(), args);
		  System.exit(exitCode); 
	  }
	}
