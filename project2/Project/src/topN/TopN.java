package topN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopN {

	public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {
//		input: the content of the file
//		output: <word>, 1
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			get word list
			String[] products = value.toString().split("\\[|\\,|/|\\;|:|\\?|!|\\.|-|\\)|\\(|\\'|\"|\\*|\\$|\\]|=|\\s+");
			List<String> list = new ArrayList<String>(Arrays.asList(products));
			list.removeAll(Arrays.asList("", null));
			for (String word : list) {
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}
	
	public static class TopNReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
			    sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
		
	private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
	    String[] remainingArgs = optionParser.getRemainingArgs();
	    if ((remainingArgs.length != 2)) {
	      System.err.println("Usage: TopN <input> <output>");
	      System.exit(2);
	    }
	    List<String> otherArgs = new ArrayList<String>();
	    for (int i=0; i < remainingArgs.length; ++i) {
	      otherArgs.add(remainingArgs[i]);
	    }
	    Path tempDir = new Path("wordcount-temp-output");
	    Job job = Job.getInstance(conf, "count"); 
		job.setJarByClass(TopN.class);
		job.setMapperClass(TopNMapper.class);
		job.setCombinerClass(TopNReducer.class);
		job.setReducerClass(TopNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
	    FileOutputFormat.setOutputPath(job, tempDir);
	    job.waitForCompletion(true);
	    
	    Job sortjob = Job.getInstance(conf, "sort");
	    sortjob.setJarByClass(TopN.class);
	    FileInputFormat.addInputPath(sortjob, tempDir);
	    sortjob.setInputFormatClass(SequenceFileInputFormat.class);
	    sortjob.setMapperClass(InverseMapper.class);
	    sortjob.setNumReduceTasks(1);
	    FileOutputFormat.setOutputPath(sortjob, new Path(otherArgs.get(1)));
	    sortjob.setOutputKeyClass(IntWritable.class);
	    sortjob.setOutputValueClass(Text.class);
	    sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);

	    sortjob.waitForCompletion(true);

	    FileSystem.get(conf).delete(tempDir);
		System.exit(0);
 
	}
}
