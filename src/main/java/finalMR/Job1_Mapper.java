package finalMR;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import utils.ISO8601;

class Job1_Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {

	/**
	 * Job1_Mapper parses the input text file, and reads it line by line. The input
	 * is split based on white spaces and tabs using a string Tokenizer This Program
	 * supports input File of a particular format.
	 * the outlinks of all article_name along with its timestamp is extracted from the input file.
	 * The oulinks are separated by special characters to be identified separately.
	 * 
	 * Key: article_name Value: initial rank , timestamp, list(article1 ## article2 ## article3 ...)
	 * is passed to the reducer of job1
	 */

	private Text key_text = new Text();
	private Text value_text = new Text();
	String article_name = "";
	String time_stamp = "";
	String outlinks = "";
	String main = "";
	String first_word = "";

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// get the first line
		String line = value.toString();
		// use StringTokenizer to split the line
		StringTokenizer tokenizer = new StringTokenizer(line, " ");

		if (tokenizer.hasMoreTokens()) {
			// save the first word of the line to compare with the keyword
			first_word = tokenizer.nextToken();

			// if the line starts with "REVISION", save the article_name and timestamp
			if (first_word.equals("REVISION")) {

				// skip the second and third word
				String second_word = tokenizer.nextToken();
				String third_word = tokenizer.nextToken();

				// save the article_name and time_stamp
				article_name = tokenizer.nextToken();

				time_stamp = tokenizer.nextToken();

				main = "";
			}

			// if the line starts with "MAIN" then, save the outlinks of that particular
			// article_name whose timestamp vales  are less than the date provided by the user
			else if (first_word.equals("MAIN")) {
				try {
					org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
					String param = conf.get("maintimestamp");

					// get the input_date passed in the terminals arguments by user
					long input_date = ISO8601.toTimeMS(param);
					long time_stamp2 = ISO8601.toTimeMS(time_stamp);
					
					//compare the input date with the timestamp of the article
					if (time_stamp2 <= input_date)
					{
						//split that line based on spaces
						String mainarr[] = line.split(" ");
						HashSet<String> outlin = new HashSet<String>();

						String stored = "";
						boolean first = true;
						
						//checking for self loops by checking if the the same article_name is
						//present in its list of outlinks
						for (int i = 1; i < mainarr.length; i++) {
							if (mainarr[i].equals(article_name) == false)
								
								//if no self loop, add that link to hashset
								
								//adding it to the hash set automatically remvoes the duplicates
								outlin.add(mainarr[i]);
						}
						// traversing through the hashset
						// to add the outlinks to the string separated by special characters '##'
						
						Iterator<String> pg = outlin.iterator();
						
						// if the article has outlinks
						if (pg.hasNext()) {
							while (pg.hasNext()) {

								if (!first)
									stored += "##";
								stored = stored + pg.next();
								first = false;
							}

							main = stored;
						}
						
						// the article has no outlinks
					} else {
						main = "";
					}
				} catch (ParseException e) {
					main = "NONO";
				}

			}
			// if the line starts with TEXTDATA, this marks the end of article entry.
			// write the output for Job1_reducer.
			else if (first_word.equals("TEXTDATA")) {

				if ((main.equals("NONO") == false) && (main.equals("") == false)) {
					// use article_name as key for the reducer
					key_text.set(new String(article_name));

					// append timestamp and main outlinks together, pass them as the value
					value_text.set(new String("1.0 " + time_stamp + " " + main.trim()));
					
                     //write it to a file
					context.write(key_text, value_text);
				}

			}

		}

	}

}