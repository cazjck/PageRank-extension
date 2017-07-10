package com.rapidminer.extension.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 
 * @author Pham Duy Khanh
 *
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	/**
	 * The `reduce(...)` method is called for each <key, (Iterable of values)>
	 * pair in the grouped input. Output values must be of the same type as
	 * input values and Input keys must not be altered.
	 *
	 * Specifically, this method should take the iterable of links to a page,
	 * along with their pagerank and number of links. It should then use these
	 * to increase this page's rank by its share of the linking page's:
	 * thisPagerank += linkingPagerank> / count(linkingPageLinks)
	 *
	 * Note: remember pagerank's dampening factor.
	 *
	 * Note: Remember that the pagerank calculation MapReduce job will run
	 * multiple times, as the pagerank will get more accurate with each
	 * iteration. You should preserve each page's list of links.
	 *
	 * @param page
	 *            The individual page whose rank we are trying to capture
	 * @param values
	 *            The Iterable of other pages which link to this page, along
	 *            with their pagerank and number of links
	 * @param context
	 *            The Reducer context object, to which key-value pairs are
	 *            written
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static Double damping = 0.85d;

	@Override
	public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean isExistingWikiPage = false;
		float sumShareOtherPageRanks = 0;
		String links = "";
		for (Text value : values) {
			String pageStr = value.toString();
/*			
			if (pageStr.equals("!")) {
				isExistingWikiPage = true;
				continue;
			}*/
			// If it is a links record, add it to the links array
			if (pageStr.startsWith("! ")) {
				links = pageStr.substring(2);
				continue;
			}
			// If it is a normal record however
				// Find the pagerank and number of links for the given page
				String[] sections = pageStr.split("\t");
				float currentPageRank = Float.valueOf(sections[1]);
				int countOutLinks = Integer.valueOf(sections[2]);

				// Add the given pagerank to the running total for the other
				// pages
				sumShareOtherPageRanks += (currentPageRank / countOutLinks);
		}
/*		if (!isExistingWikiPage)
			return;*/
		// Calculate pagerank by applying the dampening to the sum
		double newRank = (1 - damping) + (damping * sumShareOtherPageRanks);

		// Add new pagerank to total
		context.write(page, new Text(newRank + "\t" + links));
	}
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		damping=PageRankDriver.DAMPING;
	}
}
