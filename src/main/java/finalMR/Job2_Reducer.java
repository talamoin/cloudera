package finalMR;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Job2_Reducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		
		/**
		 * The aim of job2 is to calculate the page rank for each article.
		 * Job2_Reducer takes output of the job1_reducer as its input.
		 *  key: article_name Value : rank and outlinks
		 *  this class counts number of outlinks and check for nodes with no outlinks.
		 * 
		 * 
		 */
		
		Text key = page;
		Double rank = 0.0;
		String outlinks_of_targetPages = "";
		Boolean firstHash = true;

		Double dampingFactor = 0.85;
		//
		for (Text valueText : values) {

			String value = valueText.toString();
			// if it starts with # :Strings 0 # character, String 1 outlinks
			// if it doesnt start with # : String 0 article_name , String 1 old rank ,String 2 article_count
			String[] strings = value.split("\t");

			// adjust the initial guess
			if (!strings[0].equals("#")) {
				if (strings.length > 1 && strings[1] != null && !strings[1].equals("")) {

					Double pagerank = Double.parseDouble(strings[1]);

					int cites = Integer.parseInt(strings[2]);

					if (cites != 0) {

						rank = rank + (pagerank / cites);
					}
				}

			}

			else {

				if (strings.length >= 2 && !strings[1].equals("")) {

					if (!firstHash) {
						outlinks_of_targetPages = outlinks_of_targetPages + "##";
					}
					outlinks_of_targetPages = outlinks_of_targetPages + strings[1];
				}

			}
		}
         
		rank = (1 - dampingFactor) + (dampingFactor * (rank));

		Text opValue = new Text("");

		if (outlinks_of_targetPages != null && !outlinks_of_targetPages.equals("")) {
			opValue = new Text(rank + "\t" + outlinks_of_targetPages);
		} else {
			opValue = new Text(rank + "");
		}

		context.write(key, opValue);

	}
}
