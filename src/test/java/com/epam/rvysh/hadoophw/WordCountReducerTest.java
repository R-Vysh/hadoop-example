package com.epam.rvysh.hadoophw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ua.unicyb.hadoop.WordCountReducer;

public class WordCountReducerTest {
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() {
		Reducer<Text, IntWritable, Text, IntWritable> reducer = new WordCountReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		List<IntWritable> input1 = new ArrayList<IntWritable>();
		input1.add(new IntWritable(1));
		input1.add(new IntWritable(1));
		input1.add(new IntWritable(1));
		List<IntWritable> input2 = new ArrayList<IntWritable>();
		input2.add(new IntWritable(1));
		reduceDriver.withInput(new Text("hadoop"), input1).withInput(new Text("hello"), input2)
		        .withOutput(new Text("hadoop"), new IntWritable(3))
		        .withOutput(new Text("hello"), new IntWritable(1)).runTest();
	}
}
