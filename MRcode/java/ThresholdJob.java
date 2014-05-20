import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.regex.Pattern;

public class ThresholdJob implements Tool
{
	private Configuration conf;
	public static final String NAME = "Threshold";
	private static final String THRESHOLD_VAL = "threshold";
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	public Configuration getConf() {
		return conf;
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Usage: Threshold <input> <output> <threshold>");
			System.exit(1);
		}

		ToolRunner.run(new ThresholdJob(new Configuration()), args);
	}
	
	public ThresholdJob(Configuration conf) {
		this.conf = conf;
	}


public int run(String[] args) throws Exception {
	conf.setInt(THRESHOLD_VAL, Integer.parseInt(args[2]));
	Job job = new Job(conf, "ThresholdCutoff");
        job.setJarByClass(ThresholdJob.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setMapperClass(ThresholdJob.ThresholdJobMapper.class);
        job.setNumReduceTasks(0);
 	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	job.setJarByClass(ThresholdJob.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,
		removeAndSetOutput(args[1]));
	return job.waitForCompletion(true) ? 1 : 0;
}

private Path removeAndSetOutput(String outputDir) throws
IOException {
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(outputDir);
	fs.delete(path, true);
	return path;
}


public static class ThresholdJobMapper extends Mapper<Object,
Text, Text, IntWritable> {
	private int threshold;
	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		threshold = context.getConfiguration().getInt(ThresholdJob.THRESHOLD_VAL, 0);
	}
	@Override
	protected void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {
                String val = value.toString();
                int charP = val.length()-1;
                while (val.charAt(charP)<='9' && val.charAt(charP)>='0') 	{
		    charP = charP-1;
		}
                int freq = Integer.parseInt(val.substring(charP+1,val.length()));
                if (freq >= threshold) {
			context.write(new Text(val.substring(0,charP-1)), new IntWritable(freq));
		}
//		if (Integer.parseInt(tokens[1]) >= threshold) {
//			context.write(new Text(tokens[0]), new IntWritable(Integer.parseInt(tokens[1])));
//		}
	}
}

}
