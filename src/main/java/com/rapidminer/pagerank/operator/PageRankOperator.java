package com.rapidminer.pagerank.operator;

import java.util.List;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.pagerank.pagerank.PageRankDriver;
import com.rapidminer.pagerank.utilities.HadoopUtilities;
import com.rapidminer.pagerank.utilities.MaxtriHelper;
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

		// Save file to local - run Hadoop on local
		HadoopUtilities.saveExampleSetToFile(exampleSet);

		ExampleSet exampleSetResult = null;
		try {
			exampleSetResult = MaxtriHelper.getDataHadoopCluster(PageRankDriver.RESULT_LOCAL);
			exampleSetResult.getAttributes().get("att1").setName("rank");
			exampleSetResult.getAttributes().get("att2").setName("title");

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
