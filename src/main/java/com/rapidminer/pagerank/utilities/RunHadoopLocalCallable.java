package com.rapidminer.pagerank.utilities;


import java.util.concurrent.Callable;

import com.rapidminer.pagerank.pagerank.PageRankDriver;


public class RunHadoopLocalCallable implements Callable<Boolean> {


	@Override
	public Boolean call() throws Exception {
		return PageRankDriver.runPageRankHadoopLocal();
	}

}
