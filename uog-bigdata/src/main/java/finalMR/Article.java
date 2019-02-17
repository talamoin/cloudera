package finalMR;

import java.util.HashSet;

import com.amazonaws.thirdparty.joda.time.DateTime;

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
		return outlinks;
	}
	public void setOutlinks(String outlinks) {
		this.outlinks = outlinks;
	}
	@Override
	public int compareTo(Article o) {
		DateTime thisx = new DateTime(this.timestamp);
		DateTime odate = new DateTime(o.timestamp);

		return thisx.compareTo(odate);
	}
	

	
	
}
