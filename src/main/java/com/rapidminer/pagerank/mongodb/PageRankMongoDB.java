package com.rapidminer.pagerank.mongodb;

import java.util.ArrayList;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.pagerank.utilities.MaxtriHelper;

public class PageRankMongoDB {
	private static MongoClient mongoClient;
	private static final String DATABASE_PAGERANK = "pagerank";
	private static final String COLLECTION_PAGERANK = "pages";
	private static final String URL = "URL";
	private static final String PAGERANK = "PageRank";
	private static final String OUTLINKS = "OutLinks";

	public static void main(String[] args) throws Exception {

		/*
		 * page.mapReduce(map,
		 * reduce).collectionName("pages").action(MapReduceAction.REPLACE).
		 * nonAtomic(false) .sharded(false);
		 */
		 //runPageRank(1);
		//ExampleSet ExampleSet = getDataPageRank(runPageRank(1));
		// saveCollection(ExampleSet);
	}

	public static MapReduceOutput runMapReduce(DBCollection collection,Double damping) {
		String map = "function () {" 
				+ " if(this.value.OutLinks != null){										\n"
				+ " 	for (var i = 0, len = this.value.OutLinks.length; i < len; i++) {	\n"
				+ " 		emit(this.value.OutLinks[i], this.value.PageRank / len); 	\n"
				+ " 	}																\n"
				+ "}																	\n"
				+ " 	emit(this.value.URL, 0);										\n"
				+ " 	emit(this.value.URL, this.value.OutLinks);						\n"
				+ " }; 																	";
		String reduce = "function (k, vals) {											\n"
				+ "	var links = [];														\n"
				+ "	var pagerank = 0.0;													\n"
				+ " 	for (var i = 0, len = vals.length; i < len; i++) {				\n"
				+ " 		if (vals[i] instanceof Array)								\n"
				+ "  			links = vals[i];										\n"
				+ "  		else														\n"
				+ " 			pagerank += vals[i];									\n"
				+ "  	}																\n"
				+ "	pagerank = 1 - 0.85 + 0.85 * pagerank;								\n"
				+ " 	return { URL: k, PageRank: pagerank, OutLinks: links };			\n"
				+ "};																	";

		MapReduceOutput out = collection.mapReduce(
				new MapReduceCommand(collection, map, reduce, COLLECTION_PAGERANK, OutputType.REPLACE, null));
		
		System.out.println(out.getDuration());
		if (out.results() != null) {
			System.out.println("Mapreduce results");
		} else {
			System.out.println("Mapreduce failed");
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	public static ExampleSet getDataPageRank(MapReduceOutput result) {
		ArrayList<ArrayList<String>> arrayList = new ArrayList<>();
		ArrayList<String> arrayList2 = new ArrayList<>();
		if (result.results() != null) {

			for (DBObject o : result.results()) {
				Object obj = o.get("value");
				try {
					if (! (obj instanceof Double)) {
						DBObject dbObject = (DBObject) obj;
						arrayList2.add(dbObject.get(URL).toString());
						arrayList2.add(dbObject.get(PAGERANK).toString());
						arrayList2.add(dbObject.get(OUTLINKS).toString());
					}
					else {
						arrayList2.add(o.get("_id").toString());
						arrayList2.add(o.get("value").toString());
						arrayList2.add("");
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(o.toString());
				}
				arrayList.add((ArrayList<String>) arrayList2.clone());
				arrayList2.clear();
			}

		}

		// result.drop();
		String[][] array2D = MaxtriHelper.convert2DString1(arrayList);
		ExampleSet exampleSetResult = ExampleSetFactory.createExampleSet(array2D);
		exampleSetResult.getAttributes().get("att1").setName(URL);
		exampleSetResult.getAttributes().get("att2").setName(PAGERANK);
		exampleSetResult.getAttributes().get("att3").setName(OUTLINKS);
		return exampleSetResult;
	}

	public static MapReduceOutput runPageRank(int teration, Double damping) {
		mongoClient = new MongoClient("localhost", 27017);
		@SuppressWarnings("deprecation")
		DB data = mongoClient.getDB(DATABASE_PAGERANK);
		DBCollection page = data.getCollection(COLLECTION_PAGERANK);
		@SuppressWarnings("unused")

		MapReduceOutput out = null;
		for (int i = 0; i < 1; i++) {
			out = runMapReduce(page,damping);
			if (out.results() != null) {
				System.out.println("Success");
			}
		}
		return out;
	}

	public static boolean saveCollection(ExampleSet exampleSet) {
		try {
			mongoClient = new MongoClient("localhost", 27017);
			@SuppressWarnings("deprecation")
			DB db = mongoClient.getDB("pagerank");
			if (db.getCollection(COLLECTION_PAGERANK) != null) {
				db.getCollection(COLLECTION_PAGERANK).drop();
			}
			DBCollection collection = db.createCollection(COLLECTION_PAGERANK, null);
			ArrayList<DBObject> documents = new ArrayList<>();
			for (Example item : exampleSet) {
				documents.add(createDocumentOperater(item));
			}
			// System.out.println(documents.toString());
			collection.insert(documents);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			return false;
		}
	}

	public static BasicDBObject createDocumentOperater(Example item) {
		BasicDBObject newDoc = new BasicDBObject();
		String outlink = item.get(OUTLINKS).toString().trim();
		String[] links = null;
		// is empty
		if (!outlink.equals("?")) {
			links = outlink.split(",");
		}
		newDoc.append("value",
				new BasicDBObject(URL, item.get(URL).toString()).append(PAGERANK, 1.0).append(OUTLINKS, links));
		return newDoc;
	}

	public static BasicDBObject createDocumentCopy(Example item) {
		BasicDBObject newDoc = new BasicDBObject();
		String outlink = item.get(OUTLINKS).toString();
		String[] links = null;
		if (!(outlink.equals("?") || outlink.isEmpty())) {
			links = outlink.split(",");
		}
		newDoc.append("value",
				new BasicDBObject(URL, item.get(URL).toString()).append(PAGERANK, 1.0).append(OUTLINKS, links));
		return newDoc;
	}

}
