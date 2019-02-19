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

		Text key = page;
		Double rank = 0.0;
		String citedPages = "";
		Boolean firstSpecialCase = true;

		Double dampingFactor = 0.85;
		for (Text valueText : values) {

			String value = valueText.toString();
			String[] strings = value.split("\t");

			// If its not a special case of '!'
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

					if (!firstSpecialCase) {
						citedPages = citedPages + "##";
					}
					citedPages = citedPages + strings[1];
				}

			}
		}

		rank = (1 - dampingFactor) + (dampingFactor * (rank));

		Text opValue = new Text("");

		if (citedPages != null && !citedPages.equals("")) {
			opValue = new Text(rank + "\t" + citedPages);
		} else {
			opValue = new Text(rank + "");
		}

		context.write(key, opValue);

	}
}
