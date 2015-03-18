package solution2a;
import java.util.ArrayList;
import java.util.List; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
//import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class TestChallenge2a {

  MapDriver<LongWritable, Text, TextPair, IntWritable> mapDriver;
  ReduceDriver<TextPair, IntWritable, TextPair, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, TextPair, IntWritable,  TextPair, IntWritable> mapReduceDriver;

  @Before
  public void setUp() {

	LinuxMessageLogEmailMapper mapper = new LinuxMessageLogEmailMapper();
    mapDriver = new MapDriver<LongWritable, Text, TextPair, IntWritable>();
    mapDriver.setMapper(mapper);

    SumReducer reducer = new SumReducer();
    reduceDriver = new ReduceDriver<TextPair, IntWritable, TextPair, IntWritable>();
    reduceDriver.setReducer(reducer);

    mapReduceDriver = new MapReduceDriver<LongWritable, Text, TextPair, IntWritable,  TextPair, IntWritable>();
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
  }

  @Test
  public void testMapperTo() {
    Text logEntry = new Text("Nov 13 06:57:34 combo sendmail[16397]: jADBq5Ac016378: to=<sb4@dresser-rand.com>,<guidry@dresser-rand.com>,<guidry@dresser-rand.com>, delay=00:04:57, xdelay=00:04:57, mailer=esmtp, pri=424109, relay=lehmail.dresser-rand.com. [194.98.13.101], dsn=4.0.0, stat=Deferred: Connection timed out with lehmail.dresser-rand.com.");
	IntWritable one = new IntWritable(1); 
	
	mapDriver.withInput(new LongWritable(0), logEntry);
	mapDriver.withOutput(new TextPair("sb4@dresser-rand.com", "to"), one); 
	mapDriver.withOutput(new TextPair("guidry@dresser-rand.com", "to"), one); 
	mapDriver.withOutput(new TextPair("guidry@dresser-rand.com", "to"), one); 
	mapDriver.runTest(true); 
  }
  
  @Test
  public void testMapperFrom() {
    Text logEntry = new Text("Nov 13 06:57:34 combo sendmail[16397]: jADBq5Ac016378: from=<sb4@dresser-rand.com>,<guidry@dresser-rand.com>,<sb4@dresser-rand.com>, delay=00:04:57, xdelay=00:04:57, mailer=esmtp, pri=424109, relay=lehmail.dresser-rand.com. [194.98.13.101], dsn=4.0.0, stat=Deferred: Connection timed out with lehmail.dresser-rand.com.");
	IntWritable one = new IntWritable(1); 
	
	mapDriver.withInput(new LongWritable(0), logEntry);
	mapDriver.withOutput(new TextPair("sb4@dresser-rand.com", "from"), one); 
	mapDriver.withOutput(new TextPair("guidry@dresser-rand.com", "from"), one); 
	mapDriver.withOutput(new TextPair("sb4@dresser-rand.com", "from"), one); 
	mapDriver.runTest(true); 
  }

  @Test
  public void testReducerTo() {
	TextPair key = new TextPair("guidry@dresser-rand.com", "to"); 
	List<IntWritable> values = new ArrayList<IntWritable>();
	values.add(new IntWritable(1)); 
	values.add(new IntWritable(1));
	
	reduceDriver.withInput(key, values);   
    reduceDriver.withOutput(new TextPair("guidry@dresser-rand.com", "to"), new IntWritable(2));
    reduceDriver.runTest(); 
  }

  @Test
  public void testMapReduce() {
	LongWritable key = new LongWritable(0); 
    Text value = new Text("Nov 13 06:57:34 combo sendmail[16397]: jADBq5Ac016378: from=<sb4@dresser-rand.com>,<guidry@dresser-rand.com>,<sb4@dresser-rand.com>, delay=00:04:57, xdelay=00:04:57, mailer=esmtp, pri=424109, relay=lehmail.dresser-rand.com. [194.98.13.101], dsn=4.0.0, stat=Deferred: Connection timed out with lehmail.dresser-rand.com.");

	mapReduceDriver.withInput(key, value); 
	mapReduceDriver.withOutput(new TextPair("guidry@dresser-rand.com", "from"), new IntWritable(1)); 
	mapReduceDriver.withOutput(new TextPair("sb4@dresser-rand.com", "from"), new IntWritable(2)); 
  } 
}
