package finalMR;

import java.text.ParseException;
import java.util.HashSet;

import com.amazonaws.thirdparty.joda.time.DateTime;

import utils.ISO8601;

public class Article implements Comparable <Article>{
	String timestamp;
	String outlinks ;
	public Article(String timestamp, String outlinks) {
		super();
		this.timestamp = timestamp;
		this.outlinks = outlinks;
	}
	public Article(String timestamp) {
		super();
		this.timestamp = timestamp;
		}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getOutlinks() {
		String s = "1.0\t";
		s=s+outlinks;
		return s;
	}
	public void setOutlinks(String outlinks) {
		this.outlinks = outlinks;
	}
	@Override
	public int compareTo(Article o) {
		String thisx =this.timestamp;
		String odate =o.timestamp;
		try {
			
		long d1= ISO8601.toTimeMS(thisx);
		long d2=ISO8601.toTimeMS(odate);
		if(d1>d2)return 1;
		else if (d1<d2) return -1;
		else return 0;
		
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 1;
		
		
	}
	

	
	
}
