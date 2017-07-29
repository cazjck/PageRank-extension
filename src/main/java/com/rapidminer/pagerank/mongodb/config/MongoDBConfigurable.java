package com.rapidminer.pagerank.mongodb.config;

import com.mongodb.client.*;
import java.io.*;
import java.security.*;
import java.security.cert.*;
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
		super.configure((Map) parameters);
		this.closeConnection();
	}

	private synchronized void closeConnection() {
		if (this.mongoClient != null) {
			this.mongoClient.close();
			this.mongoClient = null;
		}
	}

	public MongoDatabase getConnection() throws ConfigurationException {
		final String mongoDBName = this.getParameter("instance_db");
		if (mongoDBName == null || mongoDBName.trim().isEmpty()) {
			throw new ConfigurationException(I18N.getErrorMessage("error.mongo.no_db_name", new Object[0]));
		}
		final MongoDatabase mongoDB;
		synchronized (this) {
			if (this.mongoClient == null) {
				String host = this.getParameter("instance_host");
				final String port = this.getParameter("instance_port");
				if (host == null || host.trim().isEmpty()) {
					throw new ConfigurationException(
							I18N.getErrorMessage("error.mongo.no_contact_point_defined", new Object[0]));
				}
				if (port != null && !port.trim().isEmpty()) {
					host = host + ":" + port;
				}
				final String caFile = this.getParameter("instance_ca_file");
				final String caFilePassword = this.getParameter("instance_ca_file_password");
				if (caFile != null && !caFile.isEmpty()) {
					try {
						KeyStoreLoader.addKeyStoreToTrustStore(caFile, caFilePassword);
					} catch (FileNotFoundException e) {
						throw new ConfigurationException(
								I18N.getErrorMessage("error.mongo.ca_file_not_found", new Object[0]));
					} catch (IOException e2) {
						throw new ConfigurationException(
								I18N.getErrorMessage("error.mongo.ca_io_error", new Object[0]));
					} catch (NoSuchAlgorithmException e3) {
						throw new ConfigurationException(
								I18N.getErrorMessage("error.mongo.ca_no_md5_or_x509_support", new Object[0]));
					} catch (CertificateException e4) {
						throw new ConfigurationException(
								I18N.getErrorMessage("error.mongo.ca_certificate_exception", new Object[0]));
					} catch (Exception e5) {
						throw new ConfigurationException(
								I18N.getErrorMessage("error.mongo.ca_other_error", new Object[0]));
					}
				}
				final MongoClientOptions mongoClientOptions = MongoClientOptions.builder()
						.sslEnabled(Boolean.parseBoolean(this.getParameter("instance_ssl"))).sslInvalidHostNameAllowed(
								Boolean.parseBoolean(this.getParameter("instance_allow_invalid_hostnames")))
						.build();
				if (!Boolean.parseBoolean(this.getParameter("instance_auth"))) {
					this.mongoClient = new MongoClient(host, mongoClientOptions);
				} else {
					final String user = this.getParameter("instance_user");
					final String pwd = this.getParameter("instance_pwd");
					if (user == null || user.trim().isEmpty() || pwd == null || pwd.trim().isEmpty()) {
						throw new ConfigurationException(
								I18N.getErrorMessage("error.no_auth_data_defined", new Object[0]));
					}
					final MongoCredential credential = MongoCredential.createCredential(user, mongoDBName,
							pwd.toCharArray());
					this.mongoClient = new MongoClient(new ServerAddress(host), Arrays.asList(credential),
							mongoClientOptions);
				}
			}
			mongoDB = this.mongoClient.getDatabase(mongoDBName);
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