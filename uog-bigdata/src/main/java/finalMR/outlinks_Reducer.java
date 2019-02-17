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
public class outlinks_Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> value, Context context2) throws IOException, InterruptedException {
		/*
		 * int sum = 0; //value.get() for (IntWritable value: values) sum +=value.get();
		 * context.write(key, new IntWritable(sum));
		 */
		String timestamp = "";
		DateTime y = new DateTime("2007-10-18T20:32:06Z");
		HashSet<String> outlinks = new HashSet<String>();
		Iterator<Text> val = value.iterator();
		ArrayList<Article> to_sort = new ArrayList<Article>();
		//ArrayList<String> to_sort= new ArrayList<String>();
		
		String parsed = "";

		while (val.hasNext()) {
			String line = val.next().toString();

			StringTokenizer tokenizer = new StringTokenizer(line,"[ \t]");
			boolean first = true;
			
			 timestamp = tokenizer.nextToken();
			// long d=ISO8601.toTimeMS(timestamp);
			// long y=ISO8601.toTimeMS(iso8601string)
			//String lineel[] = line.split("\\s+");
			//if (lineel.length > 0) {
				// String artname = lineel[0];
				//timestamp = lineel[0];
				DateTime hmm = new DateTime(timestamp);

				if (hmm.isBefore(y)) {
					// if (hmm.isBeforeNow()) {

					// parsed=" "+timestamp+" ";
					// parsed = " 1";
					while (tokenizer.hasMoreTokens()) {
					
					String temp = tokenizer.nextToken();

					/*
					 * if ((outlinks.add(temp)==true)&&(temp.equals(key.toString())==false)) { //if
					 * (!first) //parsed = parsed + "," + temp; //parsed = }
					 */
					//for (int i = 1; i < lineel.length; i++) {

						//String temp = lineel[i];
					if ((temp.equals(key.toString()) == false)) {
						outlinks.add(temp);
					}
					}
					//lineel=null;
					
					Iterator<String> pg = outlinks.iterator();
					String initialPageRank = "1\t";
					String stored = initialPageRank;

					while (pg.hasNext()) {
						if (!first)
							stored += ",";
						stored += pg.next();
						first = false;
					}
					to_sort.add(new Article(timestamp, stored));
					// stored="";
					timestamp = "";

				}

			}
		
		Collections.sort(to_sort, Collections.reverseOrder());
		for (int i = 0; i < to_sort.size(); i++) {
			DateTime d1 = new DateTime(to_sort.get(i).timestamp);
			DateTime d2 = new DateTime("2006-10-18T20:32:06Z");
			if (d1.isBefore(d2)) {
				// after we sort

				Text word2 = new Text(to_sort.get(i).outlinks);
				// word2.set(parsed);
				context2.write(key, word2);
				break;}
		}
	}

}