package com.rapidminer.pagerank.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProcessPageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String pageRankAndOutlinks = value.toString();
		String outLinks;
		String currPg;
		String[] pages;
		int outLinksCount = 0;
		String[] splits = pageRankAndOutlinks.split("\\t");
		String currPageAndPageRank = splits[0] + "\t" + splits[1] + "\t";
		/*
		 * Ignoring if page contains no out links, which is used to compute page
		 * ranks only for the documents in the corpus
		 */
		if (splits.length <= 2) {//|| splits[2] == ""
			context.write(new Text(splits[0]), new Text("|"));
			return;
		}
		outLinks = splits[2];
		pages = outLinks.split(",");
		outLinksCount = pages.length;
		currPg = currPageAndPageRank + outLinksCount;
		// For each out links, store page, pageRank and totalNumberOfOutLinks
		for (String page : pages) {
			context.write(new Text(page), new Text(currPg));
		}
		// Preserving the original links
		context.write(new Text(splits[0]), new Text("|" + outLinks));
	}
}
