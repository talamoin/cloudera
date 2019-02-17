package finalMR;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class PR_Reducer extends Reducer<Text, Text, Text, Text> {

    /**
     * The `reduce(...)` method is called for each <key, (Iterable of values)> pair in the grouped input.
     * Output values must be of the same type as input values and Input keys must not be altered.
     * <p/>
     * Specifically, this method should take the iterable of links to a page, along with their pagerank and number of links.
     * It should then use these to increase this page's rank by its share of the linking page's:
     * thisPagerank +=  linkingPagerank> / count(linkingPageLinks)
     * <p/>
     * Note: remember pagerank's dampening factor.
     * <p/>
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param page    The individual page whose rank we are trying to capture
     * @param values  The Iterable of other pages which link to this page, along with their pagerank and number of links
     * @param context The Reducer context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        Text key = page;
        Double rank = 0.0;
        String citedPages = "";
        Boolean firstSpecialCase = true;
        //Declaring Damping Factor
        Double dampingFactor = 0.85;
        for (Text valueText : values) {

            String value = valueText.toString();
            String[] strings = value.split("\t");

            //If its not a special case of '!'
            if (!strings[0].equals("!")) {
                if (strings.length > 1 && strings[1] != null && !strings[1].equals("")) {
                    //Getting rank
                    Double pagerank = Double.parseDouble(strings[1]);
                    //Getting number of citations
                    int cites = Integer.parseInt(strings[2]);
                    if (cites != 0) {
                        //Adding rank/cites of each citations
                        rank = rank + (pagerank / cites);
                    }
                }

            }
            // Special case when '!' comes as rank.
            else {
                if (strings.length >= 2 && !strings[1].equals("")) {
                    // Adding comma to citedPage list if its not the first occurence
                    if (!firstSpecialCase) {
                        citedPages = citedPages + ",";
                    }
                    citedPages = citedPages + strings[1];
                }

            }
        }

        // Calculating the rank as per the PageRank formula
        rank = (1 - dampingFactor) + dampingFactor * (rank);

        // Rounding the output to 4 decimals
        DecimalFormat df = new DecimalFormat("###.####");

        Text opValue = new Text("");

        if (citedPages != null && !citedPages.equals("")) {
            opValue = new Text(df.format(rank) + "\t" + citedPages);
        } else {
            opValue = new Text(df.format(rank));
        }
        // Writing the output with key as the page and value as the calculated rank
        context.write(key, opValue);
        // System.out.println("(" + key + ",\"" + df.format(rank) + citedPages + "\")");
    }
}