import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
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

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.regex.Pattern;

public class NGramJobIMC implements Tool
{
	private Configuration conf;
	public static final String NAME = "ngram";
	private static final String GRAM_LENGTH_FROM = "number_of_grams_from";
	private static final String GRAM_LENGTH_TO = "number_of_grams_to";
        private final static IntWritable one = new IntWritable(1);
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	public Configuration getConf() {
		return conf;
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 3) {
			System.err.println("Usage: ngram <input> <output> <number_of_grams_from> <number_of_grams_to> <num_reducers>");
			System.exit(1);
		}

		ToolRunner.run(new NGramJobIMC(new Configuration()), args);
	}
	
	public NGramJobIMC(Configuration conf) {
		this.conf = conf;
	}


public int run(String[] args) throws Exception {
	String jobTitle = new String();
	conf.setInt(GRAM_LENGTH_FROM, Integer.parseInt(args[2]));
	if (args.length == 4) { 
		conf.setInt(GRAM_LENGTH_TO, Integer.parseInt(args[3])); 
		jobTitle = "NGram"+args[2]+"-"+args[3];
	}
        else {
		conf.setInt(GRAM_LENGTH_TO, Integer.parseInt(args[2]));
		jobTitle = "NGram"+args[2];
	}
	conf.set("mapred.textoutputformat.separator", "\t");
	conf.set("mapred.child.java.opts", "-Xmx768m");
	Job job = new Job(conf, jobTitle);
        job.setJarByClass(NGramJobIMC.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setMapperClass(NGramJobIMC.NGramMapper.class);
	job.setCombinerClass(NGramJobIMC.IntSumReducer.class);
	job.setReducerClass(NGramJobIMC.IntSumReducer.class);
        job.setNumReduceTasks(30);
        if (args.length == 5) 
		 job.setNumReduceTasks(Integer.parseInt(args[4]));
 	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	job.setJarByClass(NGramJobIMC.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, removeAndSetOutput(args[1]));
	return job.waitForCompletion(true) ? 1 : 0;
}

private Path removeAndSetOutput(String outputDir) throws
IOException {
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(outputDir);
	fs.delete(path, true);
	return path;
}


public static class NGramMapper extends Mapper<LongWritable,
Text, Text, IntWritable> {
	private int gram_length_from, gram_length_to;
	private StringBuilder gramBuilder = new StringBuilder();
	private Text word = new Text();
	private IntWritable intCount = new IntWritable();
	private Map<String, Integer> wordMap;
        private String pad;
	private int wordMapSz = 0;			// size of wordMap
        private final short IMC_n_thres = 35;		// after this value for n don't perform in-memory combine
	private final static IntWritable one = new IntWritable(1);
        private StringBuilder removedAcc = new StringBuilder();	// contains key value without accents (for Greek)

	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		gram_length_from = context.getConfiguration().getInt(NGramJobIMC.GRAM_LENGTH_FROM, 0);
		gram_length_to = context.getConfiguration().getInt(NGramJobIMC.GRAM_LENGTH_TO, 0);
		wordMap = new HashMap<String, Integer>();
		wordMapSz = 0;
	}

	@Override
	protected void map(LongWritable key, Text value,
	Context context) throws IOException,
	InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString().toLowerCase());
	  	String[] tokens = new String[st.countTokens()];
		int i = 0;
          	while(st.hasMoreTokens()){
			tokens[i++] = st.nextToken();
          	}

                for (int n = gram_length_from; n < gram_length_to; n++) { 
			int range = Integer.toString(gram_length_to).length() - Integer.toString(n).length(); 
	                pad = new String(new char[range]).replace("\0", "0") + Integer.toString(n) +"_";
			if (tokens.length >= n) {
				for (i = 0; i < tokens.length - n + 1; i++) {
					if (gram_length_from+1 != gram_length_to) {
						gramBuilder.setLength(0);
						gramBuilder.append(pad);
					}
					else {
						gramBuilder.setLength(0);
					}
					for (int k = i; k < i + n; k++) {
						gramBuilder.append(tokens[k]);
						gramBuilder.append(" ");
					}
                                        removeAccent(gramBuilder);
			                if (gram_length_from > IMC_n_thres) {
						this.word.set(removedAcc.toString());
						context.write(this.word, one);
					}
					else {
						Integer count = wordMap.get(removedAcc.toString());
						if (count == null) {
							wordMap.put(removedAcc.toString(), 1);
							wordMapSz = wordMapSz + (removedAcc.toString().length()+5);
						} else {
							wordMap.put(removedAcc.toString(), count+1);
						}
					}
	
				  } // for (i = 0; i < tokens.length
			} // if (tokens.length >= n) 
		} // for (int n = gram_length_from; n < gram_length_to;
                if (gram_length_from <= IMC_n_thres && wordMapSz > 10000 ) {
			for (String key1 : wordMap.keySet()) {
				this.word.set(key1);
				this.intCount.set(wordMap.get(key1));
				context.write(this.word, intCount);
			}
			this.wordMap.clear();
			wordMapSz = 0;
		} // if (wordMap.size()>...
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		for (String key : wordMap.keySet()) {
			this.word.set(key);
			this.intCount.set(wordMap.get(key));
			context.write(this.word, intCount);
		}
		this.wordMap.clear();
	}

	private void removeAccent(StringBuilder word)
        {
		removedAcc.setLength(0);
        	for (int i=0; i<word.length(); i++) {
			if (word.charAt(i) == 'ά') removedAcc.append('α');
			else if (word.charAt(i) == 'έ') removedAcc.append('ε');
			else if (word.charAt(i) == 'ή') removedAcc.append('η');
			else if (word.charAt(i) == 'ί') removedAcc.append('ι');
			else if (word.charAt(i) == 'ό') removedAcc.append('ο');
			else if (word.charAt(i) == 'ύ') removedAcc.append('υ');
			else if (word.charAt(i) == 'ώ') removedAcc.append('ω');
			else if (word.charAt(i) == 'ΐ') removedAcc.append('ι');
                        else if (word.charAt(i) == 'ΰ') removedAcc.append('υ');
 			else removedAcc.append(word.charAt(i));
		}
                return;   
	}
}

public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
{
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException 
    {
	int sum = 0;
      	for (IntWritable val : values) {
      	    sum += val.get();
      	}
        result.set(sum);
        context.write(key, result);
    }
}

}
