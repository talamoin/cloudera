package finalMR;

import finalMR.Job1_Mapper;
import finalMR.Job1_Reducer;

import finalMR.Job2_Mapper;
import finalMR.Job2_Reducer;

import finalMR.Job3_Mapper;
import utils.ISO8601;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import org.apache.hadoop.fs.*;

public class Main_class extends Configured implements Tool {
	public static String timestamp;
	public static int numberOfRounds;
	public static String filename="";

	@Override
	public int run(String[] args) throws Exception {
		// Run the first MapReduce Job, parsing links from the large dump of Wikipedia
		// pages
		try {
			filename=args[1];
			numberOfRounds = Integer.parseInt(args[2]);
		} catch (NumberFormatException e) {
			System.err.println("the number of iterations should be an integer");
			return 1; // default value
		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("the correct format is hadoop InputPath OutputPath NumberOfIterations Timestamp ");

			return 1;
		}
		timestamp = args[3];

		System.err.println(ISO8601.toTimeMS(timestamp));

		boolean isCompleted = job1(args[0], args[1]);

		// String path = args[1];

		if (!isCompleted)
			return 1;
		String lastResultPath = null;

		// Run the second MapReduce Job, calculating new pageranks from existing values
		// Run this job several times, with each iteration the pagerank value will
		// become more accurate
		for (int runs = 0; runs < numberOfRounds; runs++) {
			String inPath = filename + runs;
			lastResultPath = filename + (runs + 1);

			isCompleted = job2(inPath, lastResultPath);

			if (!isCompleted)
				return 1;
		}

		isCompleted = job3(lastResultPath, "result");

		if (!isCompleted)
			return 1;
		return 0;

	}

	// Parsing MapReduce Job 1
	public boolean job1(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		// delete previous intermediate output files if they exist
		FileSystem hdfs = FileSystem.get(conf);

		for (int i = 0; i <= numberOfRounds; i++) {
			Path temp = new Path(filename + i);
			if (hdfs.exists(temp))
				hdfs.delete(temp, true);

		}
		// delete previous output file if it exists
		Path temp = new Path("result");
		if (hdfs.exists(temp)) {
			hdfs.delete(temp, true);
		}

		// add input to mapper reducer
		conf.set("maintimestamp", Main_class.timestamp);

		Job parser_job = Job.getInstance(conf, "parser");
		parser_job.setJarByClass(Main_class.class);

		parser_job.setOutputKeyClass(Text.class);
		parser_job.setOutputValueClass(Text.class);
		parser_job.setMapperClass(Job1_Mapper.class);

		parser_job.setReducerClass(Job1_Reducer.class);

		// Map -> Reducer -> Output
		FileInputFormat.setInputPaths(parser_job, new Path(inputPath));
		FileOutputFormat.setOutputPath(parser_job, new Path(outputPath));

		return parser_job.waitForCompletion(true);

	}

	// Calculation MapReduce Job 2
	// page rank calculator
	private boolean job2(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job rankCalculator = Job.getInstance(conf, "rankCalculator");
		rankCalculator.setJarByClass(Main_class.class);

		// Input -> Mapper -> Map
		rankCalculator.setOutputKeyClass(Text.class);
		rankCalculator.setOutputValueClass(Text.class);
		rankCalculator.setMapperClass(Job2_Mapper.class);

		// Map -> Reducer -> Output
		FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
		FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));
		rankCalculator.setReducerClass(Job2_Reducer.class);

		return rankCalculator.waitForCompletion(true);
	}

	// Sorting and sanitization Map Job 3
	//pageranksorter
	private boolean job3(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job rankOrdering = Job.getInstance(conf, "rankSorter");
		rankOrdering.setJarByClass(Main_class.class);

		// Input -> Mapper -> Map
		rankOrdering.setOutputKeyClass(Text.class);
		rankOrdering.setOutputValueClass(DoubleWritable.class);
		rankOrdering.setMapperClass(Job3_Mapper.class);

		FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
		FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

		rankOrdering.setInputFormatClass(TextInputFormat.class);
		rankOrdering.setOutputFormatClass(TextOutputFormat.class);

		return rankOrdering.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Main_class(), args));

	}
}
