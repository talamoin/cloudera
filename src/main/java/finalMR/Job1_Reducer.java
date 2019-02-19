package finalMR;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.ISO8601;

/*
org.apache.hadoop.conf.Configuration conf = context2.getConfiguration();
String param = conf.get("maintimestamp");
 */
public class Job1_Reducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * Job1_reducer takes output of the job1_mapper as its input. the reducer
	 * combines article_names (key) each key corresponds to values which is a list
	 * of outlinks with different dates. The reducer gets the most updated
	 * article_name by comparing the dates with the input_date from the terminal
	 * 
	 */
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context2) throws IOException, InterruptedException {
		try {

			org.apache.hadoop.conf.Configuration conf = context2.getConfiguration();
			String param = conf.get("maintimestamp");
			String timestamp = "";
			ArrayList<Article> to_sort = new ArrayList<Article>();
			// get the input_date passed in the terminals arguments by user
			long input_date = ISO8601.toTimeMS(param);

			HashSet<String> outlin = new HashSet<String>();
			String rank = "", outlinks = "";

			for (Text valueText : value) {

				System.out.println("value " + valueText.toString());
				Article temp = new Article();
				String line = valueText.toString();
				String str = "<begin>";
				String[] tokens = line.split(" ");
				String str2 = "";
				boolean flag = true;

				System.out.println(key.toString() + " tokens.length " + tokens.length + " ; line" + line);
				if (tokens.length > 0) {
					System.out.println("first loop ");
					rank = tokens[0];

					temp.setRank(rank);

					str = str + 0 + tokens[0];
				}

				if (tokens.length > 1) {

					System.out.println("second loop ");
					timestamp = tokens[1];

					temp.setTimestamp(timestamp);

					str = str + 1 + tokens[1];
				}
				if (tokens.length > 2) {

					System.out.println("third loop ");
					str = str + 2 + tokens[2];
					temp.setOutlinks(tokens[2]);
					System.out.println(key.toString() + " ; " + tokens[2]);
					str = str + "<end>";
				}
				to_sort.add(temp);
				System.out.println(key.toString() + " " + temp.toString());

			}
			if (to_sort.size() > 0) {
				Collections.sort(to_sort, Collections.reverseOrder());
				context2.write(key, new Text(to_sort.get(0).toString()));
			}

		} catch (ParseException e) {

		}

	}
}