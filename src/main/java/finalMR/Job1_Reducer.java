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


public class Job1_Reducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * Job1_reducer takes output of the job1_mapper as its input. 
	 * Key: article_name ,  value: inital rank, timestamp, outlinks( separated by ##)
	 * The reducer sorts the dates in descending 
	 * it consider the most updated article by taking the first date from the ordered list
	 * 
	 * It writes  key: article name, value: inital rank, outlinks
	   to the file
	 */
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context2) throws IOException, InterruptedException {
		try {
            // take the timestamp set on the configuration
			org.apache.hadoop.conf.Configuration conf = context2.getConfiguration();
			String param = conf.get("maintimestamp");
			
			String timestamp = "",rank = "", outlinks = "";
			//create an arraylist
			ArrayList<Article> to_sort = new ArrayList<Article>();
			
			// get the input_date passed in the terminals arguments by user
			long input_date = ISO8601.toTimeMS(param);

			HashSet<String> outlin = new HashSet<String>();
		
            // traverse through the value : rank, timestamp, outlinks
			for (Text valueText : value) {

				System.out.println("value " + valueText.toString());
				Article temp = new Article();
			
				String line = valueText.toString();
				
				
			    //split the current value to get an array of tokens
				String[] tokens = line.split(" ");
				String str2 = "";
				boolean flag = true;
				
				
				//get rank
				if (tokens.length > 0) {
					System.out.println("first loop ");
					rank = tokens[0];
                    // add it to the Article Class object
					temp.setRank(rank);

					str = str + 0 + tokens[0];
				}
                 //get timestamp
				if (tokens.length > 1) {

					System.out.println("second loop ");
					timestamp = tokens[1];
					// add it to the Article Class object
					temp.setTimestamp(timestamp);

					str = str + 1 + tokens[1];
				}
				
				//get the outlinks
				if (tokens.length > 2) {

					System.out.println("third loop ");
					str = str + 2 + tokens[2];
					// add it to the Article Class object
					temp.setOutlinks(tokens[2]);
					
				}
				
				// add the object (rank, timestamp and outlinks) to arraylist
				to_sort.add(temp);
				System.out.println(key.toString() + " " + temp.toString());

			}
			
			//sort the arraylist in reverse order
			if (to_sort.size() > 0) {
				
				//get the first entry of the list with the largest timestamp 
				//that is the most updated article.
				Collections.sort(to_sort, Collections.reverseOrder());
				
				//write key: article_name,  value:  rank outlinks
				context2.write(key, new Text(to_sort.get(0).toString()));
			}

		} catch (ParseException e) {

		}

	}
}