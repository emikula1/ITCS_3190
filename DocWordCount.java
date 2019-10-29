package org.myorg.search;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class DocWordCount extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE 	   = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		@Override
  		public void map(LongWritable offset, Text lineText, Context context)
  							throws IOException, InterruptedException {
			String line;
			String fileName;
			String currentTerm;

						line = lineText.toString().toLowerCase();
			line = line.replaceAll("\\t", " ");


			fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			for (String term : WORD_BOUNDARY.split(line)) {
				if (term.trim().isEmpty()) {
					continue;
				}
				currentTerm = term.trim() + "#####" + fileName;
				context.write(new Text(currentTerm), ONE);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override 
		public void reduce(Text term, Iterable<IntWritable> counts, Context context)
								throws IOException, InterruptedException {
			int totalCount = 0;
			for (IntWritable count : counts) {
				totalCount += count.get();
			}

			context.write(term, new IntWritable(totalCount));
		}
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(this.getClass());
		job.setJobName(" DocumentWordCount ");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String input;
		String output;
		int status;
		input  = args[0];
		output = args[1];
		status = ToolRunner.run(new DocWordCount(), new String[] {input, output});
		
		System.exit(status);
	}

}