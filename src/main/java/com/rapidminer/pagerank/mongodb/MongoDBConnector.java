package com.rapidminer.pagerank.mongodb;

import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSONParseException;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.pagerank.mongodb.config.MongoDBConfigurable;
import com.rapidminer.pagerank.mongodb.config.MongoExceptionWrapper;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.repository.RepositoryAccessor;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.config.ConfigurationException;
import com.rapidminer.tools.config.ConfigurationManager;
import com.rapidminer.tools.config.ParameterTypeConfigurable;

public abstract class MongoDBConnector extends Operator {

	public MongoDBConnector(OperatorDescription description) {
		super(description);
	}
	
	
	@Override
	public void doWork() throws OperatorException {
		 String mongoDBInstanceName = null;
	        try {
	            mongoDBInstanceName = this.getParameterAsString("mongodb_pagerank_instance");
	             MongoDBConfigurable mongoDBInstanceConfigurable = (MongoDBConfigurable)ConfigurationManager.getInstance().lookup("mongodb_pagerank_instance", mongoDBInstanceName, (RepositoryAccessor)null);
	            MongoDatabase db=mongoDBInstanceConfigurable.getConnection();
	             doOperations(mongoDBInstanceConfigurable.getInstance(),db);
	        }
	        catch (ConfigurationException e) {
	            throw new UserError((Operator)this, (Throwable)e, "pagerank.mongodb.configuration_exception", new Object[] { mongoDBInstanceName });
	        }
	        catch (IllegalArgumentException | JSONParseException ex2) {
	            final RuntimeException ex = null;
	            final RuntimeException e2 = ex;
	            throw new UserError((Operator)this, (Throwable)e2, "pagerank.mongodb.invalid_json_object");
	        }
	        catch (MongoException e3) {
	            throw new UserError((Operator)this, (Throwable)new MongoExceptionWrapper(e3), "pagerank.mongodb.mongo_exception");
	        }
	}
	 public abstract void doOperations(final MongoClient mongoClient,MongoDatabase db) throws OperatorException;
	 
	 public List<ParameterType> getParameterTypes() {
	        final List<ParameterType> parameterTypes = (List<ParameterType>)super.getParameterTypes();
	        ParameterType type = (ParameterType)new ParameterTypeConfigurable("mongodb_pagerank_instance", I18N.getGUIMessage("operator.parameter.mongodb_instance.description", new Object[0]), "mongodb_pagerank_instance");
	        type.setOptional(false);
	        parameterTypes.add(type);
	        return parameterTypes;
	    }


}
