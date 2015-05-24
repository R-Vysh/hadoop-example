package ua.unicyb.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ua.unicyb.hadoop.WordCountDriver.LETTER_GROUPS;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String words[] = line.split(" "); // 1
		for (String word : words) {
			word = word.trim(); // 2
			if (!word.isEmpty()) {
				if (word.startsWith("a") || word.startsWith("i") || word.startsWith("e")
				        || word.startsWith("o") || word.startsWith("u")) {
					context.getCounter(LETTER_GROUPS.VOWELS).increment(1); // 3
				} else {
					context.getCounter(LETTER_GROUPS.CONSONANTS).increment(1); // 4
				}
				context.write(new Text(word), new IntWritable(1)); // 5
			}
		}
	}
}