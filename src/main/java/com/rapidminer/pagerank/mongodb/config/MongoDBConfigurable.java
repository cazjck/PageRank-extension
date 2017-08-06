package com.rapidminer.pagerank.mongodb.config;

import com.mongodb.client.*;
import com.mongodb.*;
import java.util.*;
import com.rapidminer.tools.config.*;
import com.rapidminer.tools.config.actions.*;
import com.rapidminer.pagerank.mongodb.MongoDBUtils;
import com.rapidminer.tools.*;
import java.util.logging.*;

public class MongoDBConfigurable extends AbstractConfigurable {
	private MongoClient mongoClient;

	public MongoClient getInstance() throws ConfigurationException {
		if (mongoClient == null) {
			MongoDBConfigurable.this.getConnection();
		}
		return mongoClient;
	}

	public MongoDBConfigurable() {
		this.mongoClient = null;
	}

	public void configure(final Map<String, String> parameters) {
		super.configure(parameters);
		this.closeConnection();
	}

	private synchronized void closeConnection() {
		if (this.mongoClient != null) {
			this.mongoClient.close();
			this.mongoClient = null;
		}
	}

	public MongoDatabase getConnection() throws ConfigurationException {
		String mongoDBName = this.getParameter("instance_db");
		if (mongoDBName == null || mongoDBName.trim().isEmpty()) {
			throw new ConfigurationException(I18N.getErrorMessage("error.mongo.no_db_name", new Object[0]));
		}
		MongoDatabase mongoDB = null;
		synchronized (this) {
			if (this.mongoClient == null) {
				String host = this.getParameter("instance_host");
				String port = this.getParameter("instance_port");
				if (host == null || host.trim().isEmpty()) {
					throw new ConfigurationException(
							I18N.getErrorMessage("error.mongo.no_contact_point_defined", new Object[0]));
				}
				if (port != null && !port.trim().isEmpty()) {
					host = host + ":" + port;
				}
				MongoClientOptions mongoClientOptions = MongoClientOptions.builder().build();
				if (!Boolean.parseBoolean(this.getParameter("instance_auth"))) {
					this.mongoClient = new MongoClient(host, mongoClientOptions);
				} else {
					String user = this.getParameter("instance_user");
					String pwd = this.getParameter("instance_pwd");
					if (user == null || user.trim().isEmpty() || pwd == null || pwd.trim().isEmpty()) {
						throw new ConfigurationException(
								I18N.getErrorMessage("error.no_auth_data_defined", new Object[0]));
					}
					MongoCredential credential = MongoCredential.createCredential(user, mongoDBName, pwd.toCharArray());
					this.mongoClient = new MongoClient(new ServerAddress(host), Arrays.asList(credential),
							mongoClientOptions);
				}
			}
			mongoDB = this.mongoClient.getDatabase(mongoDBName);
			if (mongoDB == null) {
				throw new ConfigurationException(
						I18N.getErrorMessage("error.mongo.database_does_not_exist", new Object[0]));
			}
		}
		MongoDBUtils.testConnection(mongoDB);
		return mongoDB;
	}

	public TestConfigurableAction getTestAction() {
		return new TestConfigurableAction() {
			public ActionResult doWork() {
				try {
					MongoDBConfigurable.this.getConnection();
					return (ActionResult) new SimpleActionResult(
							I18N.getGUIMessage("gui.test.mongodb.success_message", new Object[0]),
							ActionResult.Result.SUCCESS);
				} catch (ConfigurationException e) {
					return (ActionResult) new SimpleActionResult(e.getMessage(), ActionResult.Result.FAILURE);
				} catch (Exception e2) {
					LogService.getRoot().log(Level.WARNING, "Failed to connect to MongoDB.", e2);
					return (ActionResult) new SimpleActionResult(
							I18N.getGUIMessage("gui.test.mongodb.failure_message", new Object[0]),
							ActionResult.Result.FAILURE);
				}
			}
		};
	}

	public String getTypeId() {
		return "mongodb_pagerank_instance";
	}

	public void setParameter(final String key, final String value) {
		super.setParameter(key, value);
		this.closeConnection();
	}
}