package com.rapidminer.extension.pagerank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Pham Duy Khanh
 */
public class PageRankMapper extends Mapper<Object, Text, Text, Text> {
	/**
	 * The `map(...)` method is executed against each item in the input split. A
	 * key-value pair is mapped to another, intermediate, key-value pair.
	 *
	 * Specifically, this method should take Text objects in the form `
	 * "[page] [initialPagerank] outLinkA,outLinkB,outLinkC..."` and store a new
	 * key-value pair mapping linked pages to this page's name, rank and total
	 * number of links: `"[otherPage] [thisPage] [thisPagesRank]
	 * [thisTotalNumberOfLinks]"
	 *
	 * Note: Remember that the pagerank calculation MapReduce job will run
	 * multiple times, as the pagerank will get more accurate with each
	 * iteration. You should preserve each page's list of links.
	 *
	 * @param key
	 *            the key associated with each item output from
	 *            {@link uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseReducer
	 *            PageLinksParseReducer}
	 * @param value
	 *            the text value "[page] [initialPagerank]
	 *            outLinkA,outLinkB,outLinkC..."
	 * @param context
	 *            Mapper context object, to which key-value pairs are written
	 * @throws IOException
	 * @throws InterruptedException
	 */

	public void map(Object key, Text value, Context context) {
		try {
			// Gets the string of the value
			String valueStr = value.toString();
			// split data
			String[] sections = valueStr.split("\t");
			// Gets the mapped page
			String id = sections[0];
			// Get title
			String title = sections[1];
			String page=id+"|"+title;
			// get array outlinks
			// Gets the [thisPage] [thisPagesRank]
			String mappedPageStr = page + "\t" + 1.0;
			String outLinks = sections[2];

			// Ignore if page not enough property
			// Ignore if page contains no links
			if (sections.length < 3 || outLinks.equals("?")) {
				/*context.write(new Text(id), new Text(PageRankDriver.LINKS_SEPARATOR + title + "\t" + "?"));
				return;*/
				// Mark page as an Existing page (ignore red wiki-links)
		        context.write(new Text(page), new Text("! "));
		        return;
			}

			// Gets the linked pages and [thisTotalNumberOfLinks]
			String[] allOtherPages = outLinks.split(",");
			int total = allOtherPages.length;
			 
			// For each linked to page, store [thisPage] [thisPagesRank] [thisTotalNumberOfLinks]
			for (String otherPage : allOtherPages) {
				context.write(new Text(otherPage), new Text(mappedPageStr + "\t" + total));
			}

			// Adds original links for preservation
			context.write(new Text(page), new Text("! " + outLinks));

		} catch (Exception e) {
			System.out.println(value.toString());
		}
	}
}