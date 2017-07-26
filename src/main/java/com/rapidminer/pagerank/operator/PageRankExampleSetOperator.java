package com.rapidminer.pagerank.operator;

import java.util.List;

import com.mongodb.MapReduceOutput;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.AbstractExampleSetProcessing;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.operator.error.AttributeNotFoundError;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.pagerank.hadoop.PageRankDriver;
import com.rapidminer.pagerank.mongodb.MongoDBPageRank;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.Ontology;

/**
 * 
 * @author Khanh Duy Pham
 *
 */
public class PageRankExampleSetOperator extends AbstractExampleSetProcessing {

	public static final String PARAMETER_DAMPING = "Damping factor";
	public static final String PARAMETER_INTERATION = "Iteration factor";
	public static final Boolean PARAMETER_CLUSTER = false;

	public PageRankExampleSetOperator(OperatorDescription description) {
		super(description);
	}

	@Override
	protected MetaData modifyMetaData(ExampleSetMetaData metaData) throws UndefinedParameterError {
		metaData.addAttribute(new AttributeMetaData("PageRank", Ontology.NUMERICAL));
		return super.modifyMetaData(metaData);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();

		ParameterType damping = new ParameterTypeDouble(PARAMETER_DAMPING, "This parameter defines damping factory.",
				0.0, 1.0, 0.85);
		ParameterType interations = new ParameterTypeInt(PARAMETER_INTERATION,
				"This parameter defines iteration factory.", 1, 100, 2);
		// type.registerDependencyCondition(new BooleanParameterCondition(this,

		types.add(damping);
		types.add(interations);

		return types;
	}

	@Override
	public ExampleSet apply(ExampleSet exampleSet) throws OperatorException {
		// Logger logger = LogService.getRoot();
		// get value parameter
		Double damping = getParameterAsDouble(PARAMETER_DAMPING);
		int interaions = getParameterAsInt(PARAMETER_INTERATION);
		PageRankDriver.DAMPING = damping;
		PageRankDriver.INTERATIONS = interaions;
		// Validation attributes
		checkAttribute(exampleSet);
		ExampleSet exampleSetResult = null;
		try {

			// Save Example Set to MongoDB
			if (!MongoDBPageRank.saveCollection(exampleSet)) {
				throw new UserError(this, "301", "error save file");
			}

			MapReduceOutput result;
			if ((result = MongoDBPageRank.runPageRank(interaions, damping)) == null) {
				throw new UserError(this, "301", "Page Rank - Map Reduce on MongoDB failed");
			}
			exampleSetResult = MongoDBPageRank.getDataPageRank(result);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
		return exampleSetResult;
	}

	private void checkAttribute(ExampleSet exampleSet) throws OperatorException {
		Attribute URL = exampleSet.getAttributes().get("URL");
		Attribute OutLinks = exampleSet.getAttributes().get("OutLinks");
		if (URL == null) {
			throw new AttributeNotFoundError(this, "URL", "URL");
		}
		if (OutLinks == null) {
			throw new AttributeNotFoundError(this, "OutLinks", "OutLinks");
		}

	}

}
