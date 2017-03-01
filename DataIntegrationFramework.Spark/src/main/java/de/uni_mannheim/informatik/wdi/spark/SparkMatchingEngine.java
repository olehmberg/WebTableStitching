package de.uni_mannheim.informatik.wdi.spark;

import java.util.Iterator;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.joda.time.DateTime;

import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matching.MatchingRule;
import de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRule;
import de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRuleWithVoting;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.matching.blocking.Blocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.spark.processing.SparkDataProcessingEngine;

public class SparkMatchingEngine<RecordType extends Matchable, SchemaElementType extends Matchable> extends MatchingEngine<RecordType, SchemaElementType> {

	protected JavaSparkContext sparkContext = null;
	
	public void setSparkContext(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}
	
	public SparkMatchingEngine() {
		
	}
	
	public SparkMatchingEngine(JavaSparkContext sc) {
		super(new SparkDataProcessingEngine(sc));
		sparkContext = sc;
	}
	
	public void shutdown() {
		sparkContext.stop();
	}
	
	public static JavaSparkContext createSparkContextLocal(String appName, int numCores){
		SparkConf conf = new SparkConf().setAppName(appName)
	    		.setMaster("local[" + numCores + "]");
	    JavaSparkContext sc = new JavaSparkContext(conf);
		return sc;	
	}
	
	public static JavaSparkContext createSparkContextCluster(String appName, String masterURI, String sparkExecutorMemeory, String sparkExecutorCores, String pathToThisJarWithDependencies){
		SparkConf conf = new SparkConf().setAppName(appName)
	    		.setMaster(masterURI)
	    		.set("spark.executor.memory", sparkExecutorMemeory).set("spark.executor.cores", sparkExecutorCores)
	    		.set("spark.driver.maxResultSize", "0") // do not limit result size, can cause out of memory exceptions on the driver
	    		.setJars(new String[] { pathToThisJarWithDependencies });
//	    		.set("spark.rpc.message.maxSize", "400")
		
	    JavaSparkContext sc = new JavaSparkContext(conf);
		return sc;	
	}
	
	public ResultSet<Pair<String, String>> loadTextFiles(String path, int minPartitions) {
		JavaPairRDD<String, String> files = sparkContext.wholeTextFiles(path, minPartitions);
		
		JavaRDD<Pair<String, String>> pairs = files.map(new PairRddToPairResultSetMapper<String, String>());
		
		SparkResultSet<Pair<String, String>> result = new SparkResultSet<>();
		result.setData(pairs);
		
		return result;
	}
	
	/**
	 * Ensure that the dataset is stored as a spark compatible object and can be or is already distributed in the cluster
	 * @param dataset
	 * @return
	 */
	private <T extends Matchable, U> SparkDataSet<T, U> prepare(DataSet<T, U> dataset) {
		SparkDataSet<T, U> ds = null;
		
		if(dataset instanceof SparkDataSet) {
			ds = (SparkDataSet<T, U>)dataset;
		} else {
			ds = new SparkDataSet<T, U>(sparkContext, dataset);
			ds.parallelize();
		}
		
		return ds;
	}
	
	private SparkResultSet<Correspondence<SchemaElementType, RecordType>> prepare(ResultSet<Correspondence<SchemaElementType, RecordType>> result) {
		SparkResultSet<Correspondence<SchemaElementType, RecordType>> r = null;
		
		if(result!=null) {
			if(result instanceof SparkResultSet) {
				r = (SparkResultSet<Correspondence<SchemaElementType, RecordType>>)result;
			} else {
				r = SparkResultSet.parallelize(result, sparkContext);
			}
		} else {
			return null;
		}
		
		return r;
	}

	private SparkResultSet<Correspondence<RecordType,SchemaElementType>> prepareInstanceCorrespondences(ResultSet<Correspondence<RecordType,SchemaElementType>> result) {
		SparkResultSet<Correspondence<RecordType,SchemaElementType>> r = null;
		if(result != null){
			if(result instanceof SparkResultSet) {
				r = (SparkResultSet<Correspondence<RecordType,SchemaElementType>>)result;
			} else {
				r = SparkResultSet.parallelize(result, sparkContext);
			}
		}else {
			r = (SparkResultSet<Correspondence<RecordType,SchemaElementType>>)null;
		}
		
		return r;
	}
	
//	@Override
//	public ResultSet<Correspondence<RecordType, SchemaElementType>> runDuplicateDetection(
//			DataSet<RecordType, SchemaElementType> dataset, boolean symmetric,
//			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
//			MatchingRule<RecordType, SchemaElementType> rule,
//			Blocker<RecordType, SchemaElementType> blocker) {
//		
//		SparkDataSet<RecordType, SchemaElementType> ds = prepare(dataset);
//		
//		//TODO create adapter to wrap non-spark blocker
//		SparkBlocker<RecordType, SchemaElementType> b = (SparkBlocker<RecordType, SchemaElementType>)blocker; 
//		
//		SparkResultSet<Correspondence<SchemaElementType, RecordType>> c = prepare(schemaCorrespondences);
//		
//		return runDuplicateDetection(ds, symmetric, c, rule, b);
//	}
	
//	@Override
//	public ResultSet<Correspondence<SchemaElementType, RecordType>> runDuplicateBasedSchemaMatching(
//			DataSet<SchemaElementType, SchemaElementType> schema1,
//			DataSet<SchemaElementType, SchemaElementType> schema2,
//			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
//			SchemaMatchingRuleWithVoting<RecordType, SchemaElementType, SchemaElementType> rule) {
//		
//		SparkDataSet<SchemaElementType, SchemaElementType> s1 = prepare(schema1);
//		SparkDataSet<SchemaElementType, SchemaElementType> s2 = prepare(schema2);
//		SparkResultSet<Correspondence<RecordType, SchemaElementType>> c = SparkResultSet.parallelize(instanceCorrespondences, sparkContext);
//		
//		return super.runDuplicateBasedSchemaMatching(s1, s2, c, rule);
//	}
	
//	@Override
//	public ResultSet<Correspondence<RecordType, SchemaElementType>> runIdentityResolution(
//			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
//			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
//			MatchingRule<RecordType, SchemaElementType> rule,
//			Blocker<RecordType, SchemaElementType> blocker) {
//		
//		SparkDataSet<RecordType, SchemaElementType> ds1 = prepare(dataset1);
//		SparkDataSet<RecordType, SchemaElementType> ds2 = prepare(dataset2);
//
//		//TODO create adapter to wrap non-spark blocker
//		SparkBlocker<RecordType, SchemaElementType> b = (SparkBlocker<RecordType, SchemaElementType>)blocker;
//		
//		SparkResultSet<Correspondence<SchemaElementType, RecordType>> c = prepare(schemaCorrespondences);
//		return super.runIdentityResolution(ds1, ds2, c,
//				rule, b);
//	}
	
	@Override
	public ResultSet<Correspondence<RecordType, SchemaElementType>> runDuplicateDetection(
			DataSet<RecordType, SchemaElementType> dataset, boolean symmetric,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			MatchingRule<RecordType, SchemaElementType> rule,
			Blocker<RecordType, SchemaElementType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate Detection",
				new DateTime(start).toString()));

		SparkDataSet<RecordType, SchemaElementType> ds = prepare(dataset);
		SparkResultSet<Correspondence<SchemaElementType, RecordType>> c = prepare(schemaCorrespondences);
		
		SparkResultSet<Correspondence<RecordType, SchemaElementType>> result = new SparkResultSet<>();

//		dataset.parallelize();

		// run blocking
//		@SuppressWarnings("unchecked")
//		SparkBlockingResult<RecordType, SchemaElementType> blockedResult = (SparkBlockingResult<RecordType, SchemaElementType>)blocker.runBlocking(ds, symmetric, c, getProcessingEngine());
		SparkResultSet<BlockedMatchable<RecordType, SchemaElementType>> blockedResult = (SparkResultSet<BlockedMatchable<RecordType, SchemaElementType>>) blocker.runBlocking(ds, symmetric, c, getProcessingEngine());
		
		// move the function to its own class such that the matching engine does *not* become part of the closure
		SparkRecordMatchingFunction<RecordType, SchemaElementType> f = new SparkRecordMatchingFunction<>(rule);
		
		// compare the pairs using the Duplicate Detection rule
		JavaRDD<Correspondence<RecordType, SchemaElementType>> correspondences = blockedResult.getDistributedData().map(f);
		
		// remove all null values
		SparkFilterCorrespondenceFunction<RecordType, SchemaElementType> filter = new SparkFilterCorrespondenceFunction<>();
		correspondences = correspondences.filter(filter);
		
		result.setData(correspondences);

		// report total Duplicate Detection time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out
				.println(String
						.format("[%s] Duplicate Detection finished after %s; found %,d correspondences.",
								new DateTime(end).toString(),
								DurationFormatUtils.formatDurationHMS(delta),
								result.size()));

		return result;
	}
	
	@Override
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runSchemaMatching(
			DataSet<SchemaElementType, SchemaElementType> schema1,
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			MatchingRule<SchemaElementType, RecordType> rule,
			SchemaBlocker<SchemaElementType, RecordType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Label Based Schema Matching",
				new DateTime(start).toString()));

		SparkDataSet<SchemaElementType, SchemaElementType> s1 = prepare(schema1);
		SparkDataSet<SchemaElementType, SchemaElementType> s2 = prepare(schema2);
		SparkResultSet<Correspondence<RecordType, SchemaElementType>> c = prepareInstanceCorrespondences(instanceCorrespondences);
		
		SparkResultSet<Correspondence<SchemaElementType, RecordType>> result = new SparkResultSet<>();
		
		SparkResultSet<BlockedMatchable<SchemaElementType, RecordType>> blockedResult = (SparkResultSet<BlockedMatchable<SchemaElementType, RecordType>>) blocker.runBlocking(s1, s2, c, getProcessingEngine());

		
		// move the function to its own class such that the matching engine does *not* become part of the closure
		SparkRecordMatchingFunction<SchemaElementType, RecordType> f = new SparkRecordMatchingFunction<>(rule);
		f.setSchemaCorrespondences(c);
		
		// compare the pairs using the matching rule
		JavaRDD<Correspondence<SchemaElementType, RecordType>> correspondences = blockedResult.getDistributedData().map(f);
		
		// remove all null values
		SparkFilterCorrespondenceFunction<SchemaElementType, RecordType> filter = new SparkFilterCorrespondenceFunction<>();
		correspondences = correspondences.filter(filter);
		
		result.setData(correspondences);	
		
		// report total matching time
				long end = System.currentTimeMillis();
				long delta = end - start;
				System.out.println(String.format(
						"[%s] Matching finished after %s; found %,d correspondences.",
						new DateTime(end).toString(),
						DurationFormatUtils.formatDurationHMS(delta), result.size()));
		
		return result;
	}

	@Override
	public ResultSet<Correspondence<RecordType, SchemaElementType>> runIdentityResolution(
			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			MatchingRule<RecordType, SchemaElementType> rule,
			Blocker<RecordType, SchemaElementType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Matching",
				new DateTime(start).toString()));

		SparkResultSet<Correspondence<RecordType, SchemaElementType>> result = new SparkResultSet<>();

		SparkDataSet<RecordType, SchemaElementType> ds1 = prepare(dataset1);
		SparkDataSet<RecordType, SchemaElementType> ds2 = prepare(dataset2);
		SparkResultSet<Correspondence<SchemaElementType, RecordType>> c = prepare(schemaCorrespondences);
		
//		dataset1.parallelize();
//		dataset2.parallelize();
		
//		@SuppressWarnings("unchecked")
//		SparkBlockingResult<RecordType, SchemaElementType> blockedResult = (SparkBlockingResult<RecordType, SchemaElementType>)blocker.runBlocking(ds1, ds2, c, getProcessingEngine());
		SparkResultSet<BlockedMatchable<RecordType, SchemaElementType>> blockedResult = (SparkResultSet<BlockedMatchable<RecordType, SchemaElementType>>) blocker.runBlocking(ds1, ds2, c, getProcessingEngine());
		
		// move the function to its own class such that the matching engine does *not* become part of the closure
		SparkRecordMatchingFunction<RecordType, SchemaElementType> f = new SparkRecordMatchingFunction<>(rule);
		f.setSchemaCorrespondences(c);
		
		// compare the pairs using the matching rule
		JavaRDD<Correspondence<RecordType, SchemaElementType>> correspondences = blockedResult.getDistributedData().map(f);

		// remove all null values
		SparkFilterCorrespondenceFunction<RecordType, SchemaElementType> filter = new SparkFilterCorrespondenceFunction<>();
		correspondences = correspondences.filter(filter);
	
		result.setData(correspondences);
		
		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Matching finished after %s; found %,d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), result.size()));

		return result;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.MatchingEngine#runSchemaMatching(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRule)
	 */
	@Override
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runSchemaMatching(
			DataSet<SchemaElementType, SchemaElementType> schema1,
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			SchemaMatchingRule<RecordType, SchemaElementType, SchemaElementType> rule) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate-based Schema Matching",
				new DateTime(start).toString()));

		SparkDataSet<SchemaElementType, SchemaElementType> s1 = prepare(schema1);
		SparkDataSet<SchemaElementType, SchemaElementType> s2 = prepare(schema2);
		SparkResultSet<Correspondence<RecordType, SchemaElementType>> c = SparkResultSet.parallelize(instanceCorrespondences, sparkContext);
		
		SparkResultSet<Correspondence<SchemaElementType, RecordType>> result = new SparkResultSet<>();

		System.out
				.println(String
						.format("Matching %,d elements",
								instanceCorrespondences.size()));

		SparkSchemaMatchingFunction<RecordType, SchemaElementType> match = new SparkSchemaMatchingFunction<>();
		match.setRule(rule);
		match.setSchema1(s1);
		match.setSchema2(s2);

		// compare the pairs using the matching rule
		JavaRDD<ResultSet<Correspondence<SchemaElementType, RecordType>>> perRecord = c.getDistributedData().map(match);
		
		JavaRDD<Correspondence<SchemaElementType, RecordType>> allCorrespondences = perRecord.flatMap(new FlatMapFunction<ResultSet<Correspondence<SchemaElementType,RecordType>>, Correspondence<SchemaElementType,RecordType>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Correspondence<SchemaElementType, RecordType>> call(
					ResultSet<Correspondence<SchemaElementType, RecordType>> t)
					throws Exception {
				return t.get().iterator();
			}
		});
		
		result.setData(allCorrespondences);
	
		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Duplicate-based Schema Matching finished after %s; found %d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), result.size()));

		return result;
	}
	
	@Override
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runDuplicateBasedSchemaMatching(
			DataSet<SchemaElementType, SchemaElementType> schema1,
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			SchemaMatchingRuleWithVoting<RecordType, SchemaElementType, SchemaElementType> rule,
			SchemaBlocker<SchemaElementType, RecordType> schemaBlocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate-based Schema Matching",
				new DateTime(start).toString()));

		SparkDataSet<SchemaElementType, SchemaElementType> s1 = prepare(schema1);
		SparkDataSet<SchemaElementType, SchemaElementType> s2 = prepare(schema2);
		SparkResultSet<Correspondence<RecordType, SchemaElementType>> c = prepareInstanceCorrespondences(instanceCorrespondences);
		
		SparkResultSet<Pair<Correspondence<RecordType, SchemaElementType>,ResultSet<Correspondence<SchemaElementType, RecordType>>>> result = new SparkResultSet<>();

		// execute blocker
		SparkResultSet<BlockedMatchable<SchemaElementType, RecordType>> blockedResult = (SparkResultSet<BlockedMatchable<SchemaElementType, RecordType>>) schemaBlocker.runBlocking(s1, s2, c, getProcessingEngine());
		
//		for(BlockedMatchable<SchemaElementType, RecordType> b : blockedResult.get()){
//			if(b!=null)
//				for(Correspondence<RecordType, SchemaElementType> cor : b.getSchemaCorrespondences().get()){
//					if(cor!=null)
//						System.out.println(cor.getFirstRecord().getIdentifier());
//					else
//						System.out.println("Null");
//				}
//		}
		
		System.out
				.println(String
						.format("Matching %,d elements",
								instanceCorrespondences.size()));
		
		//TODO apply the rule on the blocked data
		//getProcessingEngine().transform(blockedResult, ...)
		
		// compare the pairs using the matching rule //= rule.apply(s1, s2, c);
		BlockedMatchableToPairRecordMapper<SchemaElementType, RecordType> recordMapper1 = new BlockedMatchableToPairRecordMapper<>(rule);
		ResultSet<Pair<Correspondence<RecordType, SchemaElementType>, Correspondence<SchemaElementType, RecordType>>> perRecord = getProcessingEngine().transform(blockedResult, recordMapper1);
		
		// aggregate results: voting
		//TODO aggregate currently does not use the engine for its loops, so it cannot be distributed using spark ...
		ResultSet<Correspondence<SchemaElementType, RecordType>> finalResult = rule.aggregate(perRecord, c.size(), getProcessingEngine());
		
		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Duplicate-based Schema Matching finished after %s; found %d correspondences from %,d elements.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), finalResult.size(), result.size()));

		return SparkResultSet.parallelize(finalResult, sparkContext);
	}

}
