package com.rapidminer.pagerank.mongodb.config;

import com.rapidminer.tools.config.*;
import java.util.*;
import com.rapidminer.parameter.conditions.*;
import com.rapidminer.parameter.*;

public class MongoDBConfigurator extends AbstractConfigurator<MongoDBConfigurable> {
	public static final String TYPE_ID = "mongodb_pagerank_instance";
	public static final String I18N_KEY = "mongodb";
	public static final String PARAMETER_INSTANCE_HOST = "instance_host";
	public static final String PARAMETER_INSTANCE_PORT = "instance_port";
	public static final String PARAMETER_INSTANCE_DB = "instance_db";
	public static final String PARAMETER_INSTANCE_AUTH = "instance_auth";
	public static final String PARAMETER_INSTANCE_USER = "instance_user";
	public static final String PARAMETER_INSTANCE_PWD = "instance_pwd";

	public Class<MongoDBConfigurable> getConfigurableClass() {
		return MongoDBConfigurable.class;
	}

	public List<ParameterType> getParameterTypes(ParameterHandler handler) {
		List<ParameterType> parameterTypes = new LinkedList<ParameterType>();
		parameterTypes.add((ParameterType) new ParameterTypeString(PARAMETER_INSTANCE_HOST,
				"Host of the MongoDB instance.", false));

		parameterTypes.add((ParameterType) new ParameterTypeInt(PARAMETER_INSTANCE_PORT,
				"Port of the MongoDB instance.", 1, 65535, 27017));

		parameterTypes
				.add((ParameterType) new ParameterTypeString(PARAMETER_INSTANCE_DB, "Database to be used.", true));
		parameterTypes.add((ParameterType) new ParameterTypeBoolean(PARAMETER_INSTANCE_AUTH,
				"Does the MongoDB instance require authentication?", false));

		ParameterTypeString user = new ParameterTypeString(PARAMETER_INSTANCE_USER, "MongoDB user.", true);
		user.registerDependencyCondition(
				(ParameterCondition) new BooleanParameterCondition(handler, PARAMETER_INSTANCE_AUTH, true, true));

		parameterTypes.add((ParameterType) user);
		ParameterTypePassword password = new ParameterTypePassword(PARAMETER_INSTANCE_PWD, "MongoDB password.");
		password.registerDependencyCondition(
				(ParameterCondition) new BooleanParameterCondition(handler, PARAMETER_INSTANCE_AUTH, true, true));
		parameterTypes.add((ParameterType) password);
		return parameterTypes;
	}

	public String getTypeId() {
		return TYPE_ID;
	}

	public String getI18NBaseKey() {
		return I18N_KEY;
	}
}
