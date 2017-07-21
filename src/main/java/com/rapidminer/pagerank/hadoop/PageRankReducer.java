package com.rapidminer.pagerank.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

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
	private static final Logger LOG = Logger.getLogger(PageRankReducer.class);
	@Override
	public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double pageRk = 0;
		String preserveLinks = "";
		for (Text curr : values) {
			String value = curr.toString();
			// If it is a links ***** record, preserve the links
			if (value.startsWith("*")) {
				preserveLinks = value.substring(value.lastIndexOf("*") + 1);
				LOG.info("Preserved links only" + preserveLinks);
			}
			// If it is a normal record, compute the page rank
			else {
				String[] splits = value.split("\\t"); // Find the page rank and number of links for the given page
				double currentPageRank = Double.valueOf(splits[1]);
				int linkCount = Integer.valueOf(splits[2]);
				pageRk += currentPageRank / linkCount; // Sum all the in links and divide by the out degree
			}
		}
		/*
		 * condition to avoid writing the page rank values for the documents not
		 * present in the corpus
		 */
		if (preserveLinks != "") {
			// Calculate page rank by applying the damping factor to the sum
			double pageRank = (1 - damping) + damping * pageRk;
			// Add new pagerank to total
			context.write(page, new Text(pageRank + "\t" + preserveLinks));
		}

	}

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		damping = PageRankDriver.DAMPING;
	}
}
