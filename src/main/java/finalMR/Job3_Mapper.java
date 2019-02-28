package finalMR;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class Job3_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	/**
	 * The map method is executed against each item in the input split. A key-value
	 * pair is mapped to another, intermediate, key-value pair. This method takes
	 * value: article_name, finalPageRank, list of outlinks the outgoing links are
	 * discarded each page is mapped with a rank which is Double The output from
	 * this Mapper is sorted by article_name ie the key
	 * key: article_name
	 * value: rank is written by the context object
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String rank = "0.0";
		String article_name;

		// split the values based on tabs
		String[] value_text = value.toString().split("\t");

		// check if length of values is greater than 2 and th rank and article_name is
		// not null
		if (value_text.length >= 2 && !value_text[0].equals("") && !value_text[1].equals("")) {
			//article_name is the first value 
			//rank is the second value
			article_name = value_text[0];
			rank = value_text[1];

			Double rankValue = Double.parseDouble(rank);
            //write articlename and rank to the file
			context.write(new Text(article_name), new DoubleWritable(rankValue));

		}

	}

}