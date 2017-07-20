package com.rapidminer.pagerank.operator;

import java.util.List;


import com.mongodb.MapReduceOutput;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.pagerank.pagerank.PageRank;
import com.rapidminer.pagerank.pagerank.PageRankDriver;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;

/**
 * 
 * @author Khanh Duy Pham
 *
 */
public class PageRankOperator extends Operator {

	private InputPort input = getInputPorts().createPort("input");
	private OutputPort output = getOutputPorts().createPort("result");
	public static final String PARAMETER_ADVANCED = "Advanced";
	public static final String PARAMETER_DAMPING = "Damping factor";
	public static final String PARAMETER_INTERATION = "Iteration factor";
	public static final Boolean PARAMETER_CLUSTER = false;

	public PageRankOperator(OperatorDescription description) {
		super(description);
		MetaData desiredMetaData = new MetaData(ExampleSet.class);
		SimplePrecondition simplePrecondition = new SimplePrecondition(input, desiredMetaData);
		input.addPrecondition(simplePrecondition);
		getTransformer().addPassThroughRule(input, output);
		// ChangeAttributeName changeAttributeName=new
		// ChangeAttributeName(getOperatorDescription());

	}

	@Override
	public void doWork() throws OperatorException {
		// Logger logger = LogService.getRoot();
		// get value parameter
		Double damping = getParameterAsDouble(PARAMETER_DAMPING);
		int interaions = getParameterAsInt(PARAMETER_INTERATION);
		PageRankDriver.DAMPING = damping;
		PageRankDriver.INTERATIONS = interaions;

		ExampleSet exampleSet = input.getData(ExampleSet.class);
		
		ExampleSet exampleSetResult = null;
		try {
			// Save file to local - run Hadoop on local
			if (!PageRank.saveCollection(exampleSet)) {
				throw new UserError(this, "301", "error save file");
			}
			/*if (PARAMETER_CLUSTER) {
				if (!HadoopUtilities.writeHadoopCluster(PageRankDriver.INPUT_LOCAL, PageRankDriver.INPUT_CLUSTER)) {
					throw new UserError(this, "301", "Update file to HDFS failed");
				}
				if (HadoopUtilities.runHadoopCluster()) {
					if (!HadoopUtilities.checkFileExistHadoopCluster(PageRankDriver.RESULT_CLUSTER)) {
						throw new UserError(this, "301", PageRankDriver.RESULT_CLUSTER);
					}
					exampleSetResult = HadoopUtilities.getDataHadoopCluster(PageRankDriver.RESULT_CLUSTER);
				} else {
					throw new UserError(this, "301", "Run Hadoop Cluster Failed");
				}

			} else {
				if (PageRankDriver.runPageRankHadoopLocal()) {
					if (!new File(PageRankDriver.RESULT_LOCAL).exists()) {
						throw new UserError(this, "301", PageRankDriver.RESULT_LOCAL);
					}
					exampleSetResult = HadoopUtilities.getDataHadoopLocal(PageRankDriver.RESULT_LOCAL);
				} else {
					throw new UserError(this, "301", "Run Hadoop Failed:"+PageRankDriver.RESULT_LOCAL);
				}
			}*/
			MapReduceOutput result;
			if ((result = PageRank.runPageRank(10)) == null) {
				throw new UserError(this, "301", "Page Rank - Map Reduce on MongoDB failed");
			}
			exampleSetResult=PageRank.getDataPageRank(result);
			exampleSetResult.getAttributes().get("att1").setName("ID");
			exampleSetResult.getAttributes().get("att2").setName("Page Rank");
			exampleSetResult.getAttributes().get("att3").setName("Links");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

		output.deliver(exampleSetResult);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();

		/*
		 * types.add(new ParameterTypeBoolean(PARAMETER_ADVANCED,
		 * "If checked, You can set property for damping, interation. Default: Damping factory is 0.85 and Interation factory 2"
		 * , false, false));
		 */

		ParameterType damping = new ParameterTypeDouble(PARAMETER_DAMPING, "This parameter defines damping factory.",
				0.0, 1.0, 0.85);
		ParameterType interations = new ParameterTypeInt(PARAMETER_INTERATION,
				"This parameter defines iteration factory.", 1, 100, 2);
		// type.registerDependencyCondition(new BooleanParameterCondition(this,

		types.add(damping);
		types.add(interations);

		return types;
	}

}
