package finalMR;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class Job2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * The aim of job2 is to calculate the page rank for each article.
	 * Job2_Mapper takes output of the job1_reducer as its input.
	 *  key: article_name Value rank and outlinks
	 *  this class counts number of outlinks and check for nodes with no outlinks.
	 * 
	 * 
	 */

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// parse the outlinks and the values and convert them to an arrayList of string
		List<String> valueList = new ArrayList<String>();
		String r;
		String k;
		String outlinks_list;
		// split based on tabs
		String[] value_text = value.toString().split("\t");

		// check if it's not an empty list, and the first word is not an empty String
		if (value_text.length >= 1 && value_text[0] != null) {

			// take the first word (article name) and store it as the key
			k = value_text[0];

			// add the key to the list 
			valueList.add(k);

			// if it has more than two words, then consider the article name and the rank
			if (value_text.length >= 2 && !value_text[1].equals("")) {

				r = value_text[1];

				valueList.add(r);
			}
			// if it has more than three words, then consider the article_name, the rank and
			// outlinks

			if (value_text.length >= 3 && !value_text[2].equals("")) {

				outlinks_list = value_text[2];
                 // split by ##
				valueList.addAll(StringUtils.getStringCollection(outlinks_list, "##"));
			}

		}

		//  if valueList is empty
		if (valueList.isEmpty()) {
			return;
		}
        // get article_name
		Text article_name = new Text(valueList.get(0));
		valueList.remove(0);
        
		//get rank
		String rank = valueList.get(0);
		valueList.remove(0);

		//get size of the list
		String article_count = Integer.toString(valueList.size());

		// for each page add a separate entry for the initial guess
		for (String page : valueList) {
			
			context.write(new Text(page), new Text(article_name + "\t" + rank + "\t" + article_count));
		}
		
		// if it has outlinks, we added a special character "#" to check if this pages
		// outlinks exist or not
		context.write(new Text(article_name), new Text("#\t" + StringUtils.join("##", valueList)));

	}

}
