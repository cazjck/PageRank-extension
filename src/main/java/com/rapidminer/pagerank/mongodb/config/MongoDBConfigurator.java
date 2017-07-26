package com.rapidminer.pagerank.mongodb.config;


import com.rapidminer.tools.config.*;
import java.util.*;
import com.rapidminer.parameter.conditions.*;
import com.rapidminer.parameter.*;

public class MongoDBConfigurator extends AbstractConfigurator<MongoDBConfigurable>
{
    public static final String TYPE_ID = "mongodb_pagerank_instance";
    public static final String I18N_KEY = "mongodb";
    public static final String PARAMETER_INSTANCE_HOST = "instance_host";
    public static final String PARAMETER_INSTANCE_PORT = "instance_port";
    public static final String PARAMETER_INSTANCE_DB = "instance_db";
    public static final String PARAMETER_INSTANCE_AUTH = "instance_auth";
    public static final String PARAMETER_INSTANCE_USER = "instance_user";
    public static final String PARAMETER_INSTANCE_PWD = "instance_pwd";
    public static final String PARAMETER_INSTANCE_SSL = "instance_ssl";
    public static final String PARAMETER_INSTANCE_ALLOW_INVALID_HOSTNAMES = "instance_allow_invalid_hostnames";
    public static final String PARAMETER_INSTANCE_CA = "instance_import_ca_file";
    public static final String PARAMETER_INSTANCE_CA_FILE = "instance_ca_file";
    public static final String PARAMETER_INSTANCE_CA_FILE_PASSWORD = "instance_ca_file_password";
    
    public Class<MongoDBConfigurable> getConfigurableClass() {
        return MongoDBConfigurable.class;
    }
    
    public List<ParameterType> getParameterTypes(final ParameterHandler handler) {
        final List<ParameterType> parameterTypes = new LinkedList<ParameterType>();
        parameterTypes.add((ParameterType)new ParameterTypeString("instance_host", "Host of the MongoDB instance.", false));
        parameterTypes.add((ParameterType)new ParameterTypeInt("instance_port", "Port of the MongoDB instance.", 1, 65535, 27017));
        parameterTypes.add((ParameterType)new ParameterTypeString("instance_db", "Database to be used.", true));
        parameterTypes.add((ParameterType)new ParameterTypeBoolean("instance_ssl", "Does the MongoDB instance require a SSL connection?", false));
        final ParameterTypeBoolean ca = new ParameterTypeBoolean("instance_import_ca_file", "Does the MongoDB instance require a custom CA file?", false);
        ca.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition(handler, "instance_ssl", false, true));
        parameterTypes.add((ParameterType)ca);
        final ParameterTypeFile caFile = new ParameterTypeFile("instance_ca_file", "Certificate of CA. NOTE: This CA will be trusted everywhere in RapidMiner Studio for the whole session.", "jks", true);
        caFile.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition(handler, "instance_import_ca_file", true, true));
        parameterTypes.add((ParameterType)caFile);
        final ParameterTypePassword caFilePassword = new ParameterTypePassword("instance_ca_file_password", "Password of CA. NOTE: This CA will be trusted everywhere in RapidMiner Studio for the whole session.");
        caFilePassword.setExpert(true);
        caFilePassword.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition(handler, "instance_import_ca_file", false, true));
        parameterTypes.add((ParameterType)caFilePassword);
        final ParameterTypeBoolean allowInvalidHostnames = new ParameterTypeBoolean("instance_allow_invalid_hostnames", "Allows the server SSL certificate's host name to be different from the host name provided.", false);
        allowInvalidHostnames.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition(handler, "instance_ssl", false, true));
        parameterTypes.add((ParameterType)allowInvalidHostnames);
        parameterTypes.add((ParameterType)new ParameterTypeBoolean("instance_auth", "Does the MongoDB instance require authentication?", false));
        final ParameterTypeString user = new ParameterTypeString("instance_user", "MongoDB user.", true);
        user.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition(handler, "instance_auth", true, true));
        parameterTypes.add((ParameterType)user);
        final ParameterTypePassword password = new ParameterTypePassword("instance_pwd", "MongoDB password.");
        password.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition(handler, "instance_auth", true, true));
        parameterTypes.add((ParameterType)password);
        return parameterTypes;
    }
    
    public String getTypeId() {
        return "mongodb_pagerank_instance";
    }
    
    public String getI18NBaseKey() {
        return "mongodb";
    }
}
