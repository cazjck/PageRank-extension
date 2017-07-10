package com.rapidminer.pagerank.operator;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetPassThroughRule;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.SetRelation;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;

public class MyOperator extends Operator {
	private InputPort input = getInputPorts().createPort("input");
	private OutputPort ouput = getOutputPorts().createPort("result");
	public static final String PARAMETER_TEXT = "log text";
	public static final String PARAMETER_USE_CUSTOM_TEXT = "use custom text";
	/**
	 * @param description
	 */
	public MyOperator(OperatorDescription description) {
		super(description);
		MetaData desiredMetaData = new MetaData(ExampleSet.class);
		SimplePrecondition simplePrecondition = new SimplePrecondition(input, desiredMetaData);
		input.addPrecondition(simplePrecondition);
		// getTransformer().addPassThroughRule(input, ouput);
		getTransformer().addRule(new ExampleSetPassThroughRule(input, ouput, SetRelation.EQUAL) {
			@Override
			public ExampleSetMetaData modifyExampleSet(ExampleSetMetaData metaData) throws UndefinedParameterError {
				metaData.addAttribute(new AttributeMetaData("newAttribute", Ontology.REAL));
				return super.modifyExampleSet(metaData);
			}
		});
	}

	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		// create the needed attributes
		ExampleSet exampleSet = input.getData(ExampleSet.class);
		Attributes attributes = exampleSet.getAttributes();
		String newName = "newAttribute";
		Attribute targetAttribute = AttributeFactory.createAttribute(newName, Ontology.REAL);
		targetAttribute.setTableIndex(attributes.size());
		exampleSet.getExampleTable().addAttribute(targetAttribute);
		attributes.addRegular(targetAttribute);
		for (Example item : exampleSet) {
			item.setValue(targetAttribute, Math.round(Math.random() * 10 + 0.5));
			logger.log(Level.INFO, item.toString());
		}
		String text = getParameterAsString(PARAMETER_TEXT);
		logger.log(Level.INFO, text);
		ouput.deliver(exampleSet);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();

	    types.add(new ParameterTypeBoolean(
	        PARAMETER_USE_CUSTOM_TEXT,
	        "If checked, a custom text is printed to the log view.",
	        false,
	        false));

	    ParameterType type = new ParameterTypeString(
	        PARAMETER_TEXT,
	        "This parameter defines which text is logged to  the console when this operator is executed.",
	        "This is a default text",
	        false);

	    type.registerDependencyCondition(
	        new BooleanParameterCondition(
	            this, PARAMETER_USE_CUSTOM_TEXT, true, true));

	    types.add(type);

	    return types;
	}
}
