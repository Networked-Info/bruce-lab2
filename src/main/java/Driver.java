import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//these paths are for taking arguments from command line
			Path input1 = new Path(args[0]);
			Path input2 = new Path(args[1]);
			Path input3 = new Path(args[2]);
			Path output = new Path(args[3]);
				
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Inverted Index");
				
			job.setInputFormatClass(TextInputFormat.class);
			TextInputFormat.addInputPath(job, input1);
			TextInputFormat.addInputPath(job, input2);
			TextInputFormat.addInputPath(job, input3);
				
			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, output);
				
			job.setJarByClass(Driver.class);
			job.setMapperClass(TermToIndexMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setReducerClass(InvertIndexReducer.class);
				
			job.waitForCompletion(true);
	}
}
