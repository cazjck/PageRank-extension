package com.rapidminer.pagerank.mongodb;

import java.util.ArrayList;
import java.util.HashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MapReduceOutput;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.pagerank.utilities.MaxtriHelper;

public class MongoDBPageRank {
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
		//runPageRank(10, 0.85);
		// ExampleSet ExampleSet = getDataPageRank(runPageRank(1));
		// saveCollection(ExampleSet);
	}

	public static MapReduceOutput runMapReduce(DBCollection collection, Double damping) {
		HashMap<String, Object> scope = new HashMap<>();
		scope.put("DAMPING", damping);
		String map = "function () {" + " 	if(this.value.OutLinks != null){					\n"
				+ " 		for (var i = 0, len = this.value.OutLinks.length; i < len; i++) {	\n"
				+ " 			emit(this.value.OutLinks[i], this.value.PageRank / len); 		\n"
				+ " 		}																	\n"
				+ " 	}																		\n"
				+ " 	if( this.value.URL != null){											\n"
				+ " 		emit(this.value.URL, 0);											\n"
				+ " 		emit(this.value.URL, this.value.OutLinks);							\n"
				+ "		}																		\n"
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
				+ "	pagerank = 1 - DAMPING + DAMPING * pagerank;						\n"
				+ " 	return { URL: k, PageRank: pagerank, OutLinks: links };			\n"
				+ "};																	";
		MapReduceCommand mapReduceCommand = new MapReduceCommand(collection, map, reduce, COLLECTION_PAGERANK,
				OutputType.REPLACE, null);
		 mapReduceCommand.setScope(scope);
		MapReduceOutput out = collection.mapReduce(mapReduceCommand);

		System.out.println("Duration:" + out.getDuration());

		return out;
	}

	@SuppressWarnings("unchecked")
	public static ExampleSet getDataPageRank(MapReduceOutput result) {
		ArrayList<ArrayList<Object>> arrayList = new ArrayList<>();
		ArrayList<Object> arrayList2 = new ArrayList<>();
		if (result.results() != null) {

			for (DBObject o : result.results()) {
				if (o != null) {
					Object obj = o.get("value");
					try {
						if (!(obj instanceof Double)) {
							DBObject dbObject = (DBObject) obj;
							arrayList2.add(dbObject.get(URL).toString());
							arrayList2.add(dbObject.get(PAGERANK));
							arrayList2.add(dbObject.get(OUTLINKS).toString());
						} else {
							arrayList2.add(o.get("_id").toString());
							arrayList2.add(o.get("value"));
							arrayList2.add("URL not contain in this DataSet");
						}
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println(o.toString());
					}
					arrayList.add((ArrayList<Object>) arrayList2.clone());
					arrayList2.clear();
				}
			}

		}
		// after load data , delete collection
		// result.drop();
		Object[][] array2D = MaxtriHelper.convert2DObject(arrayList);
		ExampleSet exampleSetResult = ExampleSetFactory.createExampleSet(array2D);
		exampleSetResult.getAttributes().get("att1").setName(URL);
		exampleSetResult.getAttributes().get("att2").setName(PAGERANK);
		exampleSetResult.getAttributes().get("att3").setName(OUTLINKS);
		return exampleSetResult;
	}

	public static MapReduceOutput runPageRank(int iteration, Double damping) {
		@SuppressWarnings("deprecation")
		DB data = MongoConfig.getInstance().getDB(DATABASE_PAGERANK);
		DBCollection page = data.getCollection(COLLECTION_PAGERANK);
		MapReduceOutput out = null;
		for (int i = 1; i <= iteration; i++) {
			out = runMapReduce(page, damping);
			if (out.results() != null) {
				System.out.println("Interation times:" + i);
			}
		}
		return out;
	}

	public static boolean saveCollection(ExampleSet exampleSet) {
		try {
			@SuppressWarnings("deprecation")
			DB db = MongoConfig.getInstance().getDB("pagerank");
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

}
