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
/*
org.apache.hadoop.conf.Configuration conf = context2.getConfiguration();
String param = conf.get("maintimestamp");
 */
public class Job1_Reducer extends Reducer<Text, Text, Text, Text> {




	/**
	 * Job1_reducer takes output of the job1_mapper as its input.
	 * the reducer combines article_names (key) 
	 * each key corresponds to values which is a list of outlinks with different dates.
	 * The reducer gets the most updated article_name by comparing the dates with the input_date from the terminal
	 * 
	 */
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context2) throws IOException, InterruptedException {
		try {
			
			org.apache.hadoop.conf.Configuration conf = context2.getConfiguration();
			String param = conf.get("maintimestamp");
			String timestamp = "";
			ArrayList<Article>to_sort= new ArrayList<Article>();
			// get the input_date passed in the terminals arguments by user
			long input_date= ISO8601.toTimeMS(param);
			//SIterator<Text> val = value.iterator();
		
			HashSet<String> outlin= new HashSet<String>();
			String rank="", outlinks="";
			int i =0;
			// get the first outlink from the list of values
			for (Text valueText : value) {
				i++;
				System.out.println("value "+valueText.toString());
				//context2.write(key, new Text("i\t"+i+valueText.toString()));
				Article temp= new Article();
	           String line = valueText.toString();
				//StringTokenizer tokenizer= new StringTokenizer(line,"[ \t]");
				String str="<begin>";
				String []tokens=line.split(" ");
				String str2="";
				 boolean flag= true;
				

					System.out.println(key.toString()+" tokens.length "+tokens.length+" ; line"+line);
						if(tokens.length>=0) {
							System.out.println("first loop ");
				rank=tokens[0];
					
						temp.setRank(rank);

						str=str+0+tokens[0];
						}
						
						if(tokens.length>=1) {

							System.out.println("second loop ");
						timestamp =tokens[1];
						
						temp.setTimestamp(timestamp);

						str=str+1+tokens[1];
						}
						if(tokens.length>=2) {

							System.out.println("third loop ");
						str=str+2+tokens[2];
						temp.setOutlinks(tokens[2]);
						System.out.println(key.toString()+" ; "+tokens[2]);
						str=str+"<end>";
//						context2.write(key, new Text("HELLOOO"));
						}
						to_sort.add(temp);
				//context2.write(key, new Text(str));
					System.out.println(key.toString()+" "+temp.toString());
						
			}
			if(to_sort.size()>0) {
			Collections.sort(to_sort,Collections.reverseOrder());
			//System.out.println(key.toString()+" "+to_sort.get(0).toString());
			context2.write(key, new Text(to_sort.get(0).toString()));}
				//if(temp.getTimestamp()!=null&&temp.)
				//str2="<hello>"+temp.getTimestamp()+temp.getRank()+temp.getOutlinks()+"</hello>";
				//context2.write(key, new Text(temp.getOutlinks()));
				/*
				if(tokenizer.hasMoreTokens())
				timestamp=tokenizer.nextToken();

				if(tokenizer.hasMoreTokens())
				rank= tokenizer.nextToken();

				if(tokenizer.hasMoreTokens()) {
				outlinks=tokenizer.nextToken();
				*/
				
				/*
				String all[] = outlinks.split(",");
				for (int i =0;i<all.length;i++) {
					outlin.add(all[i]);
				}
				
				}
				
				Iterator<String> pg = outlin.iterator();
				// assign initial page rank of the articles to 1
				

				// append to outlinks string, in the below format
				// "1\t[Outlink1],[Outlink2],[Outlink3]"
				while (pg.hasNext()) {
					str =str+pg.next()+ ",";
					}
					*/
				//Text w2= new Text(timestamp+" "+rank+" "+outlinks);
				//context2.write(key,w2);
			//	Article o=new Article(timestamp,outlinks);
				//to_sort.add(o);
				//context2.write(key, new Text (timestamp+" "+outlinks));
				
			
			//if(to_sort.size()>0) {
			//Collections.sort(to_sort,Collections.reverseOrder());
			/*
				for(int i=0;i<to_sort.size();i++) {
				Article object=to_sort.get(i);
				
				Text w2= new Text(object.outlinks);
				context2.write(key,w2);
			}
			*/
			/*
			while(val.hasNext()) {
				//set first word flag to true
				boolean first = true;
				//convert it to String
				String line = val.next().toString();

				
				//split it with tabs as delimiters
				//[ \t]
				StringTokenizer tokenizer = new StringTokenizer(line, "[ \t]");
				
				//context2.write(key, new Text(tokenizer.nextToken()));
				if (tokenizer.hasMoreTokens()) {
				timestamp=tokenizer.nextToken();
				long st= ISO8601.toTimeMS(timestamp);
				if(st<=input_date) {
					
					String junk ="";
					while(tokenizer.hasMoreTokens()) {
					junk=junk+tokenizer.nextToken()+' ';
					}
					//to_sort.add(new Article (timestamp,junk));
					context2.write(key, new Text(junk));
				}
				}
			}*/
			
			/*//uncomment this
			if(!to_sort.isEmpty()&&to_sort.size()>=1) {
			Collections.sort(to_sort);
			Article object=to_sort.get(0);
			String s=object.timestamp+" 1.0 ";
			if(!object.outlinks.equals("")) {
			s=s+object.outlinks+"\n\n";
			context2.write(key, new Text(s));
			s="";
			}
			else {
				context2.write(key, new Text(s));
				s="";
			}
			}
			*/
			
		
		}
		catch (ParseException e) {

		}


	}
}