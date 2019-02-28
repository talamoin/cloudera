package finalMR;

import java.text.ParseException;
import java.util.HashSet;
import com.amazonaws.thirdparty.joda.time.DateTime;
import utils.ISO8601;

public class Article implements Comparable<Article> {
	

	/* This is a setter-getter class
	* This is used to set and get 3 objects;  timestamp, outlinks,rank
	* this class also implements compareto method that compares timestamps
	*/
	String timestamp;
	String outlinks = "";
	String rank;

	public Article() {

	}

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

	public void setRank(String rank) {
		this.rank = rank;
	}

	public String getOutlinks() {
		return outlinks;
	}

	public String getRank() {
		return rank;
	}

	public void setOutlinks(String outlinks) {
		this.outlinks = outlinks;
	}

	@Override
	public int compareTo(Article o) {
		String thisx = this.timestamp;
		String odate = o.timestamp;
		try {
			if (!thisx.isEmpty() && !odate.isEmpty()) {
				long d1 = ISO8601.toTimeMS(thisx);
				long d2 = ISO8601.toTimeMS(odate);
				if (d1 > d2)
					return 1;
				else if (d1 < d2)
					return -1;
				else
					return 0;
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 1;

	}

	@Override
	public String toString() {
		return "1.0\t" + outlinks;
	}

}
