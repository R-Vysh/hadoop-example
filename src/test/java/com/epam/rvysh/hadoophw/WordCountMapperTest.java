package com.epam.rvysh.hadoophw;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import ua.unicyb.hadoop.WordCountDriver.LETTER_GROUPS;
import ua.unicyb.hadoop.WordCountMapper;

public class WordCountMapperTest {
	MapDriver<Object, Text, Text, IntWritable> mapDriver;

	@Before
	public void setUp() {
		WordCountMapper mapper = new WordCountMapper();
		mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.addInput(new LongWritable(1), new Text("hadoop     hello  "));
		mapDriver.addInput(new LongWritable(2), new Text("hadoop"));
		mapDriver.addOutput(new Text("hadoop"), new IntWritable(1));
		mapDriver.addOutput(new Text("hello"), new IntWritable(1));
		mapDriver.addOutput(new Text("hadoop"), new IntWritable(1));
		mapDriver.withCounter(LETTER_GROUPS.CONSONANTS, 3);
		mapDriver.withCounter(LETTER_GROUPS.VOWELS, 0);
		mapDriver.runTest();
	}
}
