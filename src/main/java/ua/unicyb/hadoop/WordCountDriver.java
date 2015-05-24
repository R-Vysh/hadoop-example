package ua.unicyb.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {

	public static enum LETTER_GROUPS {
		VOWELS, CONSONANTS
	};

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("def_input_path", args[0]);
		conf.set("def_output_path", args[1]);
		int res = ToolRunner.run(conf, new WordCountDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String inPath = conf.get("def_input_path");
		String outPath = conf.get("def_output_path");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length == 0) {
			System.out.println("Using default input and output path");
		}
		if (otherArgs.length == 1) {
			System.out.println("Using default output path");
			inPath = otherArgs[0];
		} else {
			inPath = otherArgs[0];
			outPath = otherArgs[1];
		}
		Job job = Job.getInstance(conf, "Word count");
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		boolean result = job.waitForCompletion(true);
		Counters counters = job.getCounters();
		Counter consonants = counters.findCounter(LETTER_GROUPS.CONSONANTS);
		System.out.println(consonants.getDisplayName() + ":" + consonants.getValue());
		Counter vowels = counters.findCounter(LETTER_GROUPS.VOWELS);
		System.out.println(vowels.getDisplayName() + ":" + vowels.getValue());
		return result ? 0 : 1;
	}
}