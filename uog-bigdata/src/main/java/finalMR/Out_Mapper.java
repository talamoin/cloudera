package finalMR;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class Out_Mapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    /**
     * The `map(...)` method is executed against each item in the input split. A key-value pair is
     * mapped to another, intermediate, key-value pair.
     * <p/>
     * Specifically, this method should take Text objects in the form:
     * `"[page]    [finalPagerank]    outLinkA,outLinkB,outLinkC..."`
     * discard the outgoing links, parse the pagerank to a float and map each page to its rank.
     * <p/>
     * Note: The output from this Mapper will be sorted by the order of its keys.
     *
     * @param key     the key associated with each item output from {@link uk.ac.ncl.cs.csc8101.hadoop.calculate.RankCalculateReducer RankCalculateReducer}
     * @param value   the text value "[page]  [finalPagerank]   outLinkA,outLinkB,outLinkC..."
     * @param context Mapper context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String rank = "0.0";
        String page;
        String[] fullList = value.toString().split("\t");
        if (fullList.length >= 2 && !fullList[0].equals("") && !fullList[1].equals("")) {
            page = fullList[0];
            rank = fullList[1];
      //      System.out.println("Key:" + rank);
            Float rankValue = Float.parseFloat(rank);
     //       System.out.println("Value:" + page);
            //Writing the rank and page to output
            context.write(new FloatWritable(rankValue), new Text(page));
        }

    }


}