package invertedIndices;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;

 
public class InvertedIndices {

	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}

	    public TextArrayWritable(String[] strings) {
	    	super(Text.class);
	    	Text[] texts = new Text[strings.length];
	    	for (int i = 0; i < strings.length; i++) {
	    		texts[i] = new Text(strings[i]);
	    	}
	    	set(texts);
	    }
	    @Override
	    public String toString() {
	    	return Arrays.toString(get());
	    	}
	    }

//    public static class myComparator extends Comparator {
//        @SuppressWarnings("rawtypes")
//        public int compare( WritableComparable a,WritableComparable b){
//            return -super.compare(a, b);
//        }
//        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            return -super.compare(b1, s1, l1, b2, s2, l2);
//        }
//    }

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
//		input: the content of the file
//		output: <word, filename>, 1
		private FileSplit split;
		private Text newKey = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			get the name of the file
			split = (FileSplit) context.getInputSplit();
			int splitIndex = split.getPath().toString().indexOf("file");
			String filename = split.getPath().toString().substring(splitIndex);
//			get word list
			String[] products = value.toString().split("\\[|\\,|/|\\;|:|\\?|!|\\.|-|\\)|\\(|\\'|\"|\\*|\\$|\\]|=|\\s+");
			List<String> list = new ArrayList<String>(Arrays.asList(products));
			list.removeAll(Arrays.asList("", null));
			for (String word : list) {
				newKey.set(word + "," + filename);
				context.write(newKey, new Text("1"));
			}
		}
	}
	
    public static class Combine extends Reducer<Text, Text, Text, Text> {
//		input: <word, filename>, 1
//		output: word, <filename, n>
        private Text info = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
            	sum += Integer.parseInt(value.toString());
            }
            String[] splitIndex = key.toString().split(",");
            String word = splitIndex[0];
            String filename = splitIndex[1];
            info.set(filename.toString() + ": " + sum+ " ");
            context.write(new Text(word.toString()), info);
        }
    }
 
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text fileList = new Text();
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
	        String result = new String();
            for (Text filePair : value) {
            	result += filePair.toString() + "; ";
            } 
            fileList.set(result);
			context.write(new Text(key), fileList);
		}
	}
	
//	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//		 
//		public void reduce(Text key, Iterable<IntWritable> value, Context context)
//				throws IOException, InterruptedException {
//			int sum = 0;
//			for (IntWritable values : value) {
//				sum += 1;
//			}
//			context.write(new Text(key), new IntWritable(sum));
//		}
//	}
 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		final Log LOG = LogFactory.getLog(InvertedIndices.class);
 
	    Configuration conf = new Configuration();
	    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
	    String[] remainingArgs = optionParser.getRemainingArgs();
	    if ((remainingArgs.length != 2)) {
	      System.err.println("Usage: InvertedIndices <input> <output>");
	      System.exit(2);
	    }
	    List<String> otherArgs = new ArrayList<String>();
	    for (int i=0; i < remainingArgs.length; ++i) {
	      otherArgs.add(remainingArgs[i]);
	    }
	    Job job = Job.getInstance(conf, "InvertedIndices"); 
		job.setJarByClass(InvertedIndices.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
//		job.setSortComparatorClass(myComparator.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
 
	}
}