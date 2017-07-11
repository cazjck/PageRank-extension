package com.rapidminer.pagerank.pagerank;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.rapidminer.pagerank.utilities.HadoopCluster;
import com.rapidminer.pagerank.utilities.HadoopUtilities;

public class PageRankDriver extends Configured implements Tool {
	public static String input;
	public static String LINKS_SEPARATOR = "|";
	private static NumberFormat nf = new DecimalFormat("00");
	public static String INPUT_LOCAL = "D:/pageRank/input/input.txt";
	public static final String OUTPUT__LOCAL = "D:/pageRank/ranking/iter";
	public static final String RESULT_LOCAL = "D:/pageRank/result";
	public static final String INPUT_CLUSTER = HadoopCluster.DEFAULT_FS + "/input/input.txt";
	public static final String OUTPUT_CLUSTER = "D:/pageRank/result";
	public static final String RESULT_CLUSTER = "D:/pageRank/result";
	public static Double DAMPING = 0.85d;
	public static int INTERATIONS = 1;
	public static Boolean CLUSTER = false;

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRankDriver(), args));

	}

	public PageRankDriver() {
	}

	public PageRankDriver(String inputPath, String outputPath, int interation, float damping) {

	}

	@Override
	public int run(String[] args) throws Exception {
		// nlwiki-latest-pages-articles
		// boolean isCompleted = runXmlParsing("wiki/in",
		// "wiki/ranking/iter00");
		Configuration conf = (CLUSTER) ? conf = HadoopCluster.getConf() : new Configuration();
		boolean isCompleted = false;

		String lastResultPath = null;
		String inPath = null;
		for (int runs = 0; runs < INTERATIONS; runs++) {
			if (runs > 0) {
				inPath = OUTPUT__LOCAL + nf.format(runs);
			} else {
				inPath = INPUT_LOCAL;
			}

			lastResultPath = OUTPUT__LOCAL + nf.format(runs + 1);

			isCompleted = runRankCalculation(conf, inPath, lastResultPath);

			if (!isCompleted)
				return 1;
		}

		isCompleted = runRankOrdering(conf, lastResultPath, RESULT_LOCAL);
		if (!isCompleted)
			return 1;
		return 0;
	}

	public static int runHadoop(String inputPath, String outputPath) throws Exception {
		Configuration conf = (CLUSTER) ? conf = HadoopCluster.getConf() : new Configuration();
		boolean isCompleted = false;
		String lastResultPath = null;
		String inPath = null;
		for (int runs = 0; runs < INTERATIONS; runs++) {
			if (runs > 0) {
				inPath = OUTPUT__LOCAL + nf.format(runs);
			} else {
				inPath = INPUT_LOCAL;
			}

			lastResultPath = OUTPUT__LOCAL + nf.format(runs + 1);

			isCompleted = runRankCalculation(conf, inPath, lastResultPath);

			if (!isCompleted)
				return 1;
		}

		isCompleted = runRankOrdering(conf, lastResultPath, RESULT_LOCAL);
		if (!isCompleted)
			return 1;
		return 0;
	}

	public static boolean runRankCalculation(Configuration conf, String inputPath, String outputPath) throws Exception {

		Job job = Job.getInstance(conf, "DBLP Input Calculate");
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(RankCalculateMapper.class);

		job.setReducerClass(RankCalculateReduce.class);
		// job.setCombinerClass(DBLPCalculateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		HadoopUtilities.deleteFolderHadoopCluster(conf, outputPath);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? true : false;
	}

	private static boolean runRankOrdering(Configuration conf, String inputPath, String outputPath) throws Exception {

		Job rankOrdering = Job.getInstance(conf, "rankOrdering");
		rankOrdering.setJarByClass(PageRankDriver.class);

		rankOrdering.setOutputKeyClass(FloatWritable.class);
		rankOrdering.setOutputValueClass(Text.class);

		rankOrdering.setMapperClass(RankingMapper.class);
		rankOrdering.setSortComparatorClass(DescendingKeyComparator.class);
		HadoopUtilities.deleteFolderHadoopCluster(conf, outputPath);
		FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
		FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

		rankOrdering.setInputFormatClass(TextInputFormat.class);
		rankOrdering.setOutputFormatClass(TextOutputFormat.class);

		return rankOrdering.waitForCompletion(true);
	}

}
