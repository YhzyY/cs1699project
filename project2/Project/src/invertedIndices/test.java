package invertedIndices;
import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class test {

	public static void main(String[] args) throws Exception {
		String values = "This eBook is for the use\" of anyone? (anywhere= at) no* cost$ and with";
		String[] products = values.split("\\[|\\,|/|\\;|:|\\?|!|\\.|-|\\)|\\(|\\'|\"|\\*|\\$|\\]|=|\\s+");
		for (String word : products) {
			System.out.println(word.toString());
		}
    }
}