package com.rapidminer.pagerank.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * 
 * @author Pham Duy Khanh
 */
public class PageRankMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * The `map(...)` method is executed against each item in the input split. A
	 * key-value pair is mapped to another, intermediate, key-value pair.
	 *
	 * Specifically, this method should take Text objects in the form ` "[page]
	 * [initialPagerank] outLinkA,outLinkB,outLinkC..."` and store a new
	 * key-value pair mapping linked pages to this page's name, rank and total
	 * number of links: `"[otherPage] [thisPage] [thisPagesRank]
	 * [thisTotalNumberOfLinks]"
	 *
	 * Note: Remember that the pagerank calculation MapReduce job will run
	 * multiple times, as the pagerank will get more accurate with each
	 * iteration. You should preserve each page's list of links.
	 */
	private static final Logger LOG = Logger.getLogger(PageRankMapper.class);
	public void map(Object key, Text value, Context context) {
		try {
			int pageTabIndex = value.find("\t");
			int rankTabIndex = value.find("\t", pageTabIndex + 1);

			String page = Text.decode(value.getBytes(), 0, pageTabIndex);
			String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex + 1);
			String links = Text.decode(value.getBytes(), rankTabIndex + 1, value.getLength() - (rankTabIndex + 1));

			// Skip pages with no links.
			if (rankTabIndex == -1 || links.isEmpty()) {
				// Mark page as an Existing page (ignore red wiki-links)
				context.write(new Text(page), new Text("*****"));
				return;
			}

			String[] allOtherPages = links.split(",");
			int totalLinks = allOtherPages.length;

			for (String otherPage : allOtherPages) {
				Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
				context.write(new Text(otherPage), pageRankTotalLinks);
			}

			// Put the original links of the page for the reduce output
			context.write(new Text(page), new Text("*****" + links));
		} catch (Exception e) {
			System.out.println(value.toString());
		}
	}
}