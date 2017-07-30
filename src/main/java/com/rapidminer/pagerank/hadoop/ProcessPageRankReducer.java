package com.rapidminer.pagerank.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProcessPageRankReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text page, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		double dampingFactor = 0.85; // Damping Factor
		double pageRk = 0;
		String preserveLinks = "";
		for (Text curr : values) {
			String value = curr.toString();
			// If it is a links ***** record, preserve the links
			if (value.startsWith("|")) {
				preserveLinks = value.substring(1);
			}
			// If it is a normal record, compute the page rank
			else {
				String[] splits = value.split("\t"); // Find the page rank and number of links for the given page
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
			double pageRank = (1 - dampingFactor) + dampingFactor * pageRk;
			// Add new pagerank to total
			context.write(page, new Text(pageRank + "\t" + preserveLinks));
		}
	}
}
