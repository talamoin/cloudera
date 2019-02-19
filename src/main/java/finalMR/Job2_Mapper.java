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
     * Job2_Mapper takes output of the job1_reducer as its input.
     * the reducer combines article_names (key) 
     * each key corresponds to values which is a list of outlinks with different dates.
     * The reducer gets the most updated article_name by comparing the dates with the input_date from the terminal
     * 
     */
	
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        List<String> valueList = getValues(value);
        if (valueList.isEmpty()) {
            return;
        }
        
        Text sourcePage = new Text(valueList.get(0));
        valueList.remove(0);
       
        String rank = valueList.get(0);
        valueList.remove(0);
       
        String targetPageCount = Integer.toString(valueList.size());
    
        for (String page : valueList) {
           
            context.write(new Text(page), new Text(sourcePage + "\t" + rank + "\t" + targetPageCount));
        }
        System.out.println("second loop"+"!\t" + StringUtils.join(",", valueList));
   
        context.write(new Text(sourcePage), new Text("#\t" + StringUtils.join(",", valueList)));
       
    }


/*
 * This method parses the rank and the outlinks value and converts it to an List   
 */
    private static List<String> getValues(Text line) throws CharacterCodingException {

        List<String> output_list = new ArrayList<String>();
        String rank;
        String key;
        String page_list;
        //split based on tabs
        String[] fullList = line.toString().split("\t");

        
        // check if it's not an empty list, and the first word is not an empty String
        if (fullList.length >= 1 && fullList[0] != null) {
            
        	// take the first word (article name) and store it as the key
            key = fullList[0];

            // add the key to the list ??
            output_list.add(key);
         
            //if it has more than two words, then consider the article name and the rank
            if (fullList.length>=2&&!fullList[1].equals("")) {
                rank = fullList[1];
               
                output_list.add(rank);
            }
           
            if (fullList.length>=3&&!fullList[2].equals("")) {
                page_list = fullList[2];
              
                output_list.addAll(StringUtils.getStringCollection(page_list));
            }
          
        }
        /*
        // check if it's not an empty list, and the first word is not an empty String
        if (fullList.length >= 2 && fullList[0] != null) {
            
        	// take the first word (article name) and store it as the key
            key = fullList[0];

            // add the key to the list ??
            output_list.add(key);
            rank = fullList[1];
            output_list.add(rank);
            
            //if it has outlinks, add them to the output list
            if (fullList.length>=3&&!fullList[2].equals("")) {
                page_list = fullList[2];
              
                //split based on comma, and add to the output list
                output_list.addAll(StringUtils.getStringCollection(page_list));
            }
          
        }
        */
        return output_list;

    }
}
