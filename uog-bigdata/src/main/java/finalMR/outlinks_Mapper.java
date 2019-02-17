package finalMR;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.thirdparty.joda.time.DateTime;

/*
public class WordCount_v0 extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "WordCount-v0");
		job.setJarByClass(WordCount_v0.class);
		job.setMapperClass(TokenCounterMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(4);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new WordCount_v0(), args));
	}
}
 */

	/*
	 * static class Map extends
	 * org.apache.hadoop.mapreduce.Mapper<LongWritable,Text, Text, IntWritable> {
	 */
	 class outlinks_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word2 = new Text();
		String article_name = "";
		String time_stamp = "";
		String outlinks = "";
		String main = "";
		String fw = "";

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// StringTokenizer tokenizer = new StringTokenizer(line);
			/*
			 * String all[]= line.split(" ");
			 * 
			 * if(all[0].equals("REVISION")) { //key article_name=all[3]; time_stamp=all[4];
			 * time_stamp=time_stamp.substring(0,10); main="";
			 * 
			 * } else if (all[0].equals("MAIN")) { //main=Arrays.toString(all); for (int
			 * i=1;i<all.length;i++) { main=main+all[i]+" "; }
			 * 
			 * //System.out.println(Arrays.toString(Arrays.copyOfRange(o,1,o.length))); }
			 * 
			 * word.set(new String (article_name)); word2.set(new String(time_stamp+main));
			 * context.write(word, word2);
			 * 
			 */

			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			if (tokenizer.hasMoreTokens()) {
				fw = tokenizer.nextToken();

				if (fw.equals("REVISION")) {
					String sw = tokenizer.nextToken();
					String tw = tokenizer.nextToken();
					article_name = tokenizer.nextToken();
					time_stamp = tokenizer.nextToken();
					// time_stamp = time_stamp.substring(0, 10);
					main = "";
				} else if (fw.equals("MAIN")) {
					try {
						main = line.substring(4, line.length() - 1);
					} catch (StringIndexOutOfBoundsException e) {
						main = line;
					}
				} else if (fw.equals("TEXTDATA")) {
					word.set(new String(article_name));
					word2.set(new String(time_stamp + " " + main));
					context.write(word, word2);

				}

			}

			// while (tokenizer.hasMoreTokens()) {
			/*
			 * word.set(tokenizer.nextToken()); context.write(word, one);
			 */

			// 4
			// }
			/*
			 * if(all[0].equals("REVISION")) { word.set(all[1]); context.write(word, one); }
			 */
			/*
			 * if(line.isEmpty()) { word.set(a); context.write(word,one); a=""; }
			 * 
			 * String article_name=""; String time_stamp=""; String
			 * o[]=line.split("[\\s+]"); if (o[0].equals("REVISION")) { article_name = o[3];
			 * time_stamp=o[4]; time_stamp=time_stamp.substring(0,10); a=a+article_name;
			 * 
			 * 
			 * 
			 * //System.out.println(article_name+" "+time_stamp); } else if
			 * (o[0].equals("MAIN")) {
			 * //System.out.println(Arrays.toString(Arrays.copyOfRange(o,1,o.length))); }
			 */

		}

	}