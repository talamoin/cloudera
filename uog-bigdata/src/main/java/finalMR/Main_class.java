package finalMR;







import finalMR.outlinks_Mapper;
import finalMR.outlinks_Reducer;
import finalMR.Article;
import finalMR.PR_Mapper;
import finalMR.PR_Reducer;
import finalMR.Out_Mapper;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class Main_class extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

   
//not correct
    @Override
    public int run(String[] args) throws Exception {
        //Run the first MapReduce Job, parsing links from the large dump of wikipedia pages
        boolean isCompleted = job1("input/HadoopPageRank/wiki", "output/HadoopPageRank/ranking/iter00");
        if (!isCompleted) return 1;

        String lastResultPath = null;

        //Run the second MapReduce Job, calculating new pageranks from existing values
        //Run this job several times, with each iteration the pagerank value will become more accurate
        for (int runs = 0; runs < 8; runs++) {
            String inPath = "output/HadoopPageRank/ranking/iter" + nf.format(runs);
            lastResultPath = "output/HadoopPageRank/ranking/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculator(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

        isCompleted = runRankSorter(lastResultPath, "output/HadoopPageRank/result");

        if (!isCompleted) return 1;
        return 0;
    }


    //Parsing MapReduce Job 1
    public int job1(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
    	Job job = Job.getInstance(getConf(), "WordCount");
		job.setJarByClass(Main_class.class);

		job.setMapperClass(outlinks_Mapper.class);
		job.setCombinerClass(outlinks_Reducer.class);
		job.setReducerClass(outlinks_Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setNumReduceTasks(4);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return (job.waitForCompletion(true) ? 0 : 1);

       
    }

    //Calculation MapReduce Job 2
    private boolean runRankCalculator(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(Main_class.class);

        // Input -> Mapper -> Map
        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);
        rankCalculator.setMapperClass(PR_Mapper.class);

        // Map -> Reducer -> Output
        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));
        rankCalculator.setReducerClass(PR_Reducer.class);

        return rankCalculator.waitForCompletion(true);
    }

    //Sorting and sanitization Map Job 3
    private boolean runRankSorter(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankSorter");
        rankOrdering.setJarByClass(Main_class.class);

        // Input -> Mapper -> Map
        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);
        rankOrdering.setMapperClass(Out_Mapper.class);

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



