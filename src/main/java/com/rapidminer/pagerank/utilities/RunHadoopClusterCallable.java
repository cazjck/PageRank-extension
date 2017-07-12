package com.rapidminer.pagerank.utilities;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.pagerank.pagerank.PageRankDriver;

/**
 * 
 * @author Khanh Duy Kham
 *
 */
public class RunHadoopClusterCallable implements Callable<Boolean> {


	@Override
	public Boolean call() throws Exception {
		return PageRankDriver.runPageRankHadoopCluster();
	}


}
