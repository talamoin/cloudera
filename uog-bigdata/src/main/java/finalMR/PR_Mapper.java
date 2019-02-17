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

public class PR_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * The `map(...)` method is executed against each item in the input split. A key-value pair is
     * mapped to another, intermediate, key-value pair.
     * <p/>
     * Specifically, this method should take Text objects in the form
     * `"[page]    [initialPagerank]    outLinkA,outLinkB,outLinkC..."`
     * and store a new key-value pair mapping linked pages to this page's name, rank and total number of links:
     * `"[otherPage]   [thisPage]    [thisPagesRank]    [thisTotalNumberOfLinks]"
     * <p/>
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param key     the key associated with each item output from {@link uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseReducer PageLinksParseReducer}
     * @param value   the text value "[page]  [initialPagerank]   outLinkA,outLinkB,outLinkC..."
     * @param context Mapper context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        List<String> valueList = getValues(value);
        if (valueList.isEmpty()) {
            return;
        }
        // Getting the source page which is the first element in the parsed list
        Text sourcePage = new Text(valueList.get(0));
        valueList.remove(0);
        // Getting the rank
        String rank = valueList.get(0);
        valueList.remove(0);
        // Number of target pages
        String targetPageCount = Integer.toString(valueList.size());
        //System.out.println(sourcePage);
        // Iterating though each target page to generate key value pair
        for (String page : valueList) {
            // System.out.println(page + "," + key + "\t" + rank + "\t" + targetPageCount);
            // Writing output page  sourcePage  rank    targetPageCount ex: A   B   1.3 2
            context.write(new Text(page), new Text(sourcePage + "\t" + rank + "\t" + targetPageCount));
        }
        //Mapping special case of source page
       // System.out.println(key + ",!\t" + StringUtils.join(",", valueList));
        // Writing special case output sourcePage  !    targetPages ex: A   !   C,D
        context.write(new Text(sourcePage), new Text("!\t" + StringUtils.join(",", valueList)));

    }


    // Method to return the parsed values as a List, Makes the mapping easier
    private static List<String> getValues(Text ipString) throws CharacterCodingException {

        List<String> outputList = new ArrayList<String>();
        String rank;
        String key;
        String pageList;
        String[] fullList = ipString.toString().split("\t");

        if (fullList.length >= 1 && fullList[0] != null) {
            //Extracting key
            key = fullList[0];

            //   System.out.println("Key:" + key);
            outputList.add(key);
            //Extracting value of rank
            if (fullList.length>=2&&!fullList[1].equals("")) {
                rank = fullList[1];
                //Adding rank to the output list
                outputList.add(rank);
            }
            //    System.out.println("Rank:" + rank);
            //Extracting the comma separated list of target pages
            if (fullList.length>=3&&!fullList[2].equals("")) {
                pageList = fullList[2];
                //Adding pages to the output list after extracting from csv
                outputList.addAll(StringUtils.getStringCollection(pageList));
            }
            //     System.out.println("pageList:" + pageList);
//        System.out.println("OuputList:" + outputList);
//        System.out.println(outputList.get0(0));
        }
        return outputList;

    }
}
