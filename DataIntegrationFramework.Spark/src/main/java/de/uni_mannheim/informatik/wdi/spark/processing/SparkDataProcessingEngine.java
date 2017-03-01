/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.wdi.spark.processing;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;
import de.uni_mannheim.informatik.wdi.processing.ResultSetCollector;
import de.uni_mannheim.informatik.wdi.spark.SparkBasicCollectionWrapper;
import de.uni_mannheim.informatik.wdi.spark.SparkCollection;
import de.uni_mannheim.informatik.wdi.spark.SparkDataSet;
import de.uni_mannheim.informatik.wdi.spark.SparkResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SparkDataProcessingEngine extends DataProcessingEngine implements Serializable{
	
	/**
	 * class is serialized
	 */
	private static final long serialVersionUID = 1L;
	
	protected transient JavaSparkContext sparkContext = null;
	
	public SparkDataProcessingEngine(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}
	
/**
	below method, prepareBasicCollection, is used to parallelize basicCollection using sparkContext. It distributes basicCollection over cluster.   
*/
	@SuppressWarnings("unchecked")
	private <T> SparkCollection<T> prepareBasicCollection(BasicCollection<T> dataset) {
		SparkCollection<T> r = null;
		
		if(dataset instanceof SparkCollection) {
			r = (SparkCollection<T>)dataset;
			if(!r.isDistributed())
				r.parallelize();
		} else {
			r = new SparkBasicCollectionWrapper<T>(sparkContext, dataset);
			if(!r.isDistributed())
				r.parallelize();
		}
		
		return r;
	}
	
	private <T extends Matchable, U> SparkDataSet<T,U> prepareDataSet(DataSet<T, U> dataset) {
		SparkDataSet<T, U> d = null;
		
		if(dataset instanceof SparkDataSet) {
			d = (SparkDataSet<T,U>)dataset;
			if(!d.isDistributed())
				d.parallelize();
		} else {
			d = new SparkDataSet<T,U>(sparkContext, dataset);
			if(!d.isDistributed())
				d.parallelize();
		}
		
		return d;
	}
	
	public <RecordType> ResultSet<RecordType> createResultSet(RecordType dummyForTypeInference) {
		return new ResultSet<>();
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#createDataSet(de.uni_mannheim.informatik.wdi.model.Matchable, java.lang.Object)
	 */
	@Override
	public <RecordType extends Matchable, SchemaElementType> DataSet<RecordType, SchemaElementType> createDataSet(
			RecordType dummyForTypeInference, SchemaElementType secondDummy) {
		return new SparkDataSet<>();
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#createDataset(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.RecordMapper)
	 */
	@Override
	public <InputType, RecordType extends Matchable, SchemaElementType> DataSet<RecordType, SchemaElementType> createDataset(
			BasicCollection<InputType> input, final RecordMapper<InputType, RecordType> transformation) {
		SparkDataSet<RecordType, SchemaElementType> result = new SparkDataSet<>();

		JavaRDD<InputType> distData = prepareBasicCollection(input).getDistributedData();
		
		JavaRDD<RecordType> outputRecord = distData.flatMap(new FlatMapFunction<InputType, RecordType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<RecordType> call(InputType record) throws Exception {

				ResultSetCollector<RecordType> resultCollector = new ResultSetCollector<>();
				
				resultCollector.setResult(new ResultSet<RecordType>());
				
				resultCollector.initialise();
				
				transformation.mapRecord(record, resultCollector);
								
				resultCollector.finalise();
				
				return resultCollector.getResult().get().iterator(); 
			}
		});
		
		result.setDistributedData(outputRecord);
		
		return result;
	}
	
	
//	doesn`t make sense on spark
	public <RecordType> void iterateDataset(BasicCollection<RecordType> dataset, final DatasetIterator<RecordType> iterator) {
		
		JavaRDD<RecordType> distData = prepareBasicCollection(dataset).getDistributedData();
		@SuppressWarnings("unused")
		JavaRDD<RecordType> processedData = distData.map(new org.apache.spark.api.java.function.Function<RecordType, RecordType>() {

			/**
			 * performs specified operation on each record and returns it
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public RecordType call(RecordType v1) throws Exception {
				iterator.next(v1);
				return v1;
			}
		});
	}

/**
	'filter' method is used to filter the data
*/	
	public <ElementType> ResultSet<ElementType> filter(BasicCollection<ElementType> data, final Function<Boolean, ElementType> criteria) {
		SparkResultSet<ElementType> result = new SparkResultSet<>();

		JavaRDD<ElementType> distData = prepareBasicCollection(data).getDistributedData();
		
		/**		
		*		'filter' method used below is from spark which accepts 'Fucntion' with two parameters.
		*		1. type of Data 2. boolean variable
		*		if the record 'v1' satisfies the condition provided in 'execute' method then it will be added to JavaRDD else discarded 
		*		
		**/
		
		JavaRDD<ElementType> filterData = distData.filter(new org.apache.spark.api.java.function.Function<ElementType, Boolean>() {

			/**
			 * filter the records by passing each element of JavaRDD through function 'call'
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(ElementType v1) throws Exception {
				return criteria.execute(v1);
			}
		});
		
		result.setData(filterData);
		
		return result;
	}
	
	@Override
	public <RecordType> ResultSet<RecordType> assignUniqueRecordIds(ResultSet<RecordType> data, Function<RecordType, Pair<Long,RecordType>> assignUniqueId) {
		
		SparkCollection<RecordType> d = prepareBasicCollection(data);
		
		JavaRDD<RecordType> assigned = d.getDistributedData().zipWithUniqueId().map(new SparkZipWithUniqueIdAssignmentWrapper<RecordType>(assignUniqueId));
		
		SparkResultSet<RecordType> result = new SparkResultSet<>();
		result.setData(assigned);
		
		return result;
	}
	
	@Override
	public <RecordType extends Matchable, SchemaElementType> DataSet<RecordType, SchemaElementType> assignUniqueRecordIds(DataSet<RecordType, SchemaElementType> data, Function<RecordType, Pair<Long,RecordType>> assignUniqueId) {
		
		SparkDataSet<RecordType, SchemaElementType> d = prepareDataSet(data);
		
		JavaRDD<RecordType> assigned = d.getDistributedData().zipWithUniqueId().map(new SparkZipWithUniqueIdAssignmentWrapper<RecordType>(assignUniqueId));
		
		SparkDataSet<RecordType, SchemaElementType> result = new SparkDataSet<>();
		result.setDistributedData(assigned);
		
		return result;
	}
	
/**
	if joinKeygenerator is common for basicCollection1 and basicCollection2 then use 'join' method provided below.
*/
	public <KeyType, RecordType> ResultSet<Pair<RecordType,RecordType>> join(BasicCollection<RecordType> dataset1, BasicCollection<RecordType> dataset2, final Function<KeyType, RecordType> joinKeyGenerator) {
		return join(dataset1, dataset2, joinKeyGenerator, joinKeyGenerator);
	}
	
/**	
	'join' method is used to join 2 basicCollections holding the same type of data.
	joinKeygenerator1 and joinKeygenerator2 are used to create joining key for basicCollection1 and basicCollection2 respectively.
*/	
	public <KeyType, RecordType> ResultSet<Pair<RecordType,RecordType>> join(BasicCollection<RecordType> dataset1, BasicCollection<RecordType> dataset2, final Function<KeyType, RecordType> joinKeyGenerator1, final Function<KeyType, RecordType> joinKeyGenerator2) {
		SparkResultSet<Pair<RecordType, RecordType>> result = new SparkResultSet<Pair<RecordType,RecordType>>();
		
		JavaRDD<RecordType> distData1 = prepareBasicCollection(dataset1).getDistributedData();
		JavaRDD<RecordType> distData2 = prepareBasicCollection(dataset2).getDistributedData();
		
		/**	
		mapping basicCollection1 using joinKeyGenerator1 to create key for every record.
		*/	
		JavaPairRDD<KeyType, RecordType> pairDistData1 = distData1.mapToPair(new PairFunction<RecordType, KeyType, RecordType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<KeyType, RecordType> call(RecordType t)
					throws Exception {
				return new Tuple2<KeyType, RecordType>(joinKeyGenerator1.execute(t), t);
			}
		});
		
		/**		
		mapping basicCollection2 using joinKeyGenerator2 to create key for every record.
		*/
		JavaPairRDD<KeyType, RecordType> pairDistData2 = distData2.mapToPair(new PairFunction<RecordType, KeyType, RecordType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<KeyType, RecordType> call(RecordType t)
					throws Exception {
				return new Tuple2<KeyType, RecordType>(joinKeyGenerator2.execute(t), t);
			}
		});
		
		/**		
		'join' method form spark is used to join 2 basicCollection and distinct() is used to remove duplicate records.		
		*/
		JavaPairRDD<KeyType, Tuple2<RecordType, RecordType>> joined = pairDistData1.join(pairDistData2).distinct();
		
		/**	
		finally joined result of basicCollection1 and basicCollection2 is mapped to create final result set.
		here, each Pair<RecordType, RecordType> represent combination of record from basicCollection1 and basicCollection2.  
		*/
		JavaRDD<Pair<RecordType, RecordType>> pairedResult = joined.map(new org.apache.spark.api.java.function.Function<Tuple2<KeyType,Tuple2<RecordType,RecordType>>, Pair<RecordType,RecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Pair<RecordType, RecordType> call(
					Tuple2<KeyType, Tuple2<RecordType, RecordType>> v1)
					throws Exception {
				return new Pair<RecordType, RecordType>(v1._2()._1(), v1._2()._2());
			}
		});
		
		result.setData(pairedResult);
		
		return result;
	}

/**	
	'symmetricSelfJoin' is used to join a basicCollection with itself.
	joinKeygenerator is used to create a key for each record from basicCollection
*/	
	public <KeyType, RecordType> ResultSet<Pair<RecordType,RecordType>> symmetricSelfJoin(BasicCollection<RecordType> dataset, final Function<KeyType, RecordType> joinKeyGenerator) {
		
		SparkResultSet<Pair<RecordType, RecordType>> result = new SparkResultSet<Pair<RecordType,RecordType>>();
		
		JavaRDD<RecordType> distData = prepareBasicCollection(dataset).getDistributedData();
		
		/**		
		mapping basicCollection using joinKeygenerator to create a key for each record
		*/
		JavaPairRDD<KeyType, RecordType> pairDistData = distData.mapToPair(new PairFunction<RecordType, KeyType, RecordType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<KeyType, RecordType> call(RecordType t)
					throws Exception {
				return new Tuple2<KeyType, RecordType>(joinKeyGenerator.execute(t), t);
			}
		});

		/**
		joining basicCollection with itself using spark`s 'join' method
		*/
		JavaPairRDD<KeyType, Tuple2<RecordType, RecordType>> joined = pairDistData.join(pairDistData).distinct();
		
		/**		
		filter the pairs of type (record1,record1)
		*/
		JavaRDD<Tuple2<RecordType, RecordType>> filtered1 = joined.filter(new org.apache.spark.api.java.function.Function<Tuple2<KeyType,Tuple2<RecordType,RecordType>>, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(
					Tuple2<KeyType, Tuple2<RecordType, RecordType>> v1)
					throws Exception {
				if(v1._2()._1().equals(v1._2()._2()))
					return false;
				else
					return true;
			}
		}).values();
		
		/**		
		filter the pairs of type (record1,record2) and (record2,record1) by keeping one of them. 
		'reduceByKey' operation from spark is used to do this task.
		*/
		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<RecordType, RecordType>> filtered2 = filtered1.mapToPair(new PairFunction<Tuple2<RecordType,RecordType>, Tuple2<Integer, Integer>, Tuple2<RecordType,RecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Tuple2<Integer, Integer>, Tuple2<RecordType, RecordType>> call(
					Tuple2<RecordType, RecordType> t) throws Exception {
				Tuple2<Integer, Integer> tuple;
				if(t._1().hashCode() > t._2().hashCode())
					tuple = new Tuple2<Integer, Integer>(t._2().hashCode(), t._1().hashCode());
				else
					tuple = new Tuple2<Integer, Integer>(t._1().hashCode(), t._2().hashCode());
				
				return new Tuple2<Tuple2<Integer,Integer>, Tuple2<RecordType,RecordType>>(tuple, t);
			}
		}).reduceByKey(new Function2<Tuple2<RecordType,RecordType>, Tuple2<RecordType,RecordType>, Tuple2<RecordType,RecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<RecordType, RecordType> call(
					Tuple2<RecordType, RecordType> v1,
					Tuple2<RecordType, RecordType> v2) throws Exception {
				return v1;
			}
		});
		
		/**	
		finally joined result of basicCollection and basicCollection is mapped to create final result set.
		*/
		JavaRDD<Pair<RecordType, RecordType>> pairedResult = filtered2.map(new org.apache.spark.api.java.function.Function<Tuple2<Tuple2<Integer, Integer>, Tuple2<RecordType, RecordType>>, Pair<RecordType, RecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Pair<RecordType, RecordType> call(
					Tuple2<Tuple2<Integer, Integer>, Tuple2<RecordType, RecordType>> v1)
					throws Exception {
				return new Pair<RecordType, RecordType>(v1._2()._1(), v1._2()._2());
			}
		});
		
		result.setData(pairedResult);
		
		return result;
		
	}
	
/**
 * 'sort' function is used to sort the basicCollection using 'sortingKey'.
 * 	boolean variable 'ascending' represents whether sorting order should ascending. (or descending) 
*/
	public <ElementType, KeyType extends Comparable<KeyType>> ResultSet<ElementType> sort(BasicCollection<ElementType> data, final Function<KeyType, ElementType> sortingKey, final boolean ascending) {
		
		SparkResultSet<ElementType> result = new SparkResultSet<>();

		JavaRDD<ElementType> distData = prepareBasicCollection(data).getDistributedData();
		
		/**
		 * 		'sort' function from spark is used to sort basicCollection which accepts 'sortingKey' function as a parameter to provide key for sorting the records. 		
		*/
		JavaRDD<ElementType> sortedData = distData.sortBy(new org.apache.spark.api.java.function.Function<ElementType, KeyType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public KeyType call(ElementType v1) throws Exception {			
				return sortingKey.execute(v1);
			}
		}, ascending, distData.getNumPartitions()); // find out what is numPartition 
		
		result.setData(sortedData);
		
		return result;
	}
	
/**
 * 'transform' function is used to convert one type of basicCollection into other type of basicCollection.
 * function accepts a 'recordMapper' as a parameter which represents input type of basicCollection (the one that you have) and output type of basicCollection (the one into which you want to convert to) 
 * a 'recordMapper' defines a method 'mapRecord' where the provide conversion operation takes place	
*/	
	public <RecordType, OutputRecordType> ResultSet<OutputRecordType> transform(BasicCollection<RecordType> dataset, final RecordMapper<RecordType, OutputRecordType> transformation) {
		
		SparkResultSet<OutputRecordType> result = new SparkResultSet<>();

		JavaRDD<RecordType> distData = prepareBasicCollection(dataset).getDistributedData();
		
		/**		
		 *		'flatMap' function in spark is used to maintain one-to-many mapping for each record.
		 *		'resultSetCollector' is initialized with the output type of basicCollection which holds transformed record. 				
		*/
		JavaRDD<OutputRecordType> outputRecord = distData.flatMap(new FlatMapFunction<RecordType, OutputRecordType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<OutputRecordType> call(RecordType record) throws Exception {

				ResultSetCollector<OutputRecordType> resultCollector = new ResultSetCollector<>();
				
				resultCollector.setResult(new ResultSet<OutputRecordType>());
				
				resultCollector.initialise();
				
				transformation.mapRecord(record, resultCollector);
								
				resultCollector.finalise();
				
				return resultCollector.getResult().get().iterator(); 
			}
		});
		
		result.setData(outputRecord);
		
		return result;
		
	}
	
/**	
 * 'groupRecords' function is used to group the records of basicCollection.
 * it accepts the 'recordKeyValueMapper' which represents the 'groupingKeyType', input type of basicCoolection and output type of basic collection.
 * (input and output type of basicCollection can be defined similar)
*/
	public <KeyType, RecordType, OutputRecordType> ResultSet<Group<KeyType, OutputRecordType>> groupRecords(BasicCollection<RecordType> dataset, final RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy) {
		
		SparkResultSet<Group<KeyType, OutputRecordType>> result = new SparkResultSet<>();

		JavaRDD<RecordType> distData = prepareBasicCollection(dataset).getDistributedData();
		
		/**
		 * 
		 */
		JavaPairRDD<KeyType,HashSet<OutputRecordType>> groupData = distData.flatMapToPair(new PairFlatMapFunction<RecordType, KeyType, OutputRecordType>() {

			/**
			 * First, each record is mapped to pair '<Key, Record>' using 'sparkGroupCollector'.
			 * Afterwards all the records are aggregated/combined by their 'key' and groups are generated (separate group for each key with respective records)
			 * 'agrregateByKey' function from spark gives better performance than 'groupByKey' function of spark and this the reason for 'why did we use 'aggregateByKey' instead of 'groupByKey'?'
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<KeyType, OutputRecordType>> call(RecordType t)
					throws Exception {
				SparkGroupCollector<KeyType, OutputRecordType> groupCollector = new SparkGroupCollector<>();
				
				groupCollector.initialise();

				groupBy.mapRecord(t, groupCollector);
				
				groupCollector.finalise();
				
				return groupCollector.getResultData().get().iterator();
			}
		}).aggregateByKey(new HashSet<OutputRecordType>(), new Function2<HashSet<OutputRecordType>, OutputRecordType, HashSet<OutputRecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public HashSet<OutputRecordType> call(HashSet<OutputRecordType> v1,
					OutputRecordType v2) throws Exception {
				v1.add(v2);
				return v1;
			}
		}, new Function2<HashSet<OutputRecordType>, HashSet<OutputRecordType>, HashSet<OutputRecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public HashSet<OutputRecordType> call(HashSet<OutputRecordType> v1,
					HashSet<OutputRecordType> v2) throws Exception {
				v1.addAll(v2);
				return v1;
			}
		});
		
		/**
		 * finally each group is mapped to create final output. 
		 */
		JavaRDD<Group<KeyType, OutputRecordType>> finalData = groupData.map(new org.apache.spark.api.java.function.Function<Tuple2<KeyType,HashSet<OutputRecordType>>, Group<KeyType, OutputRecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Group<KeyType, OutputRecordType> call(
					Tuple2<KeyType, HashSet<OutputRecordType>> v1)
					throws Exception {
				ResultSet<OutputRecordType> result = new ResultSet<>();
				for(OutputRecordType o : v1._2())
					result.add(o);
				
				return new Group<KeyType, OutputRecordType>(v1._1(), result);
			}
		});
		
		result.setData(finalData);
	
		return result;
		
	}
	
/**
 * 'aggregateRecords' function works similar to 'groupRecords' but here additional parameter, called 'ResultType', is defined. 
 * This additional parameter is final result type of aggregation process.  
*/	
	public <KeyType, RecordType, OutputRecordType, ResultType> ResultSet<Pair<KeyType, ResultType>> aggregateRecords(BasicCollection<RecordType> dataset, final RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy, final DataAggregator<KeyType, OutputRecordType, ResultType> aggregator) {
		
		SparkResultSet<Pair<KeyType, ResultType>> result = new SparkResultSet<>();

		JavaRDD<RecordType> distData = prepareBasicCollection(dataset).getDistributedData();
		
		/**
		 * First group the records by their key (similar to 'groupRecords' function)
		 */
		JavaPairRDD<KeyType, HashSet<OutputRecordType>> groupData = distData.flatMapToPair(new PairFlatMapFunction<RecordType, KeyType, OutputRecordType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<KeyType, OutputRecordType>> call(RecordType t)
					throws Exception {
				SparkGroupCollector<KeyType, OutputRecordType> groupCollector = new SparkGroupCollector<>();
				
				groupCollector.initialise();

				groupBy.mapRecord(t, groupCollector);
				
				groupCollector.finalise();
				
				return groupCollector.getResultData().get().iterator();
			}
		}).aggregateByKey(new HashSet<OutputRecordType>(), new Function2<HashSet<OutputRecordType>, OutputRecordType, HashSet<OutputRecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public HashSet<OutputRecordType> call(HashSet<OutputRecordType> v1,
					OutputRecordType v2) throws Exception {
				v1.add(v2);
				return v1;
			}
		}, new Function2<HashSet<OutputRecordType>, HashSet<OutputRecordType>, HashSet<OutputRecordType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public HashSet<OutputRecordType> call(HashSet<OutputRecordType> v1,
					HashSet<OutputRecordType> v2) throws Exception {
				v1.addAll(v2);
				return v1;
			}
		});
		
		/**
		 * Now actual aggregation of records take place. Here, we aggregate records using 'DataAggregator' which is provided as a parameter to 'aggregtaeRecords' function.
		 * 'DataAggregator' provides the method to aggregate records and based on this method records within each group are aggregated. The resulting type of this aggregation is nothing but 'ResultType' (a type parameter) which is the final result of aggregation process.
		 */
		JavaRDD<Pair<KeyType, ResultType>> aggregateData = groupData.map(new org.apache.spark.api.java.function.Function<Tuple2<KeyType,HashSet<OutputRecordType>>, Pair<KeyType, ResultType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Pair<KeyType, ResultType> call(
					Tuple2<KeyType, HashSet<OutputRecordType>> v1)
					throws Exception {
				ResultType result = null;
				for(OutputRecordType o : v1._2()){
					result = aggregator.aggregate(result, o);
				}
				
				return new Pair<KeyType, ResultType>(v1._1(), result);
			}
		});
		
		result.setData(aggregateData);
		
		return result;
	}
	
/**
 * 'coGroup' function is useful whenever there is need to join 2 different kinds of basicCollections ('join' function does not work when definition/type of basicCollections are different). 
 * It works similar to 'join' function but here instead of one-to-one pairs of records, many-to-many pairs of records are generated. 
 */
	public <KeyType, RecordType1, RecordType2, OutputRecordType> ResultSet<OutputRecordType> coGroup(BasicCollection<RecordType1> data1, BasicCollection<RecordType2> data2, final Function<KeyType, RecordType1> groupingKeyGenerator1, final Function<KeyType, RecordType2> groupingKeyGenerator2, final RecordMapper<Pair<Iterable<RecordType1>, Iterable<RecordType2>>, OutputRecordType> resultMapper) {
		
		SparkResultSet<OutputRecordType> result = new SparkResultSet<>();
		
		JavaRDD<RecordType1> distData1 = prepareBasicCollection(data1).getDistributedData();
		JavaRDD<RecordType2> distData2 = prepareBasicCollection(data2).getDistributedData();
		
		/**	
		mapping basicCollection1 using groupingKeyGenerator1 to create key for every record.
		*/
		JavaPairRDD<KeyType, RecordType1> pair1 = distData1.mapToPair(new PairFunction<RecordType1, KeyType, RecordType1>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<KeyType, RecordType1> call(
					RecordType1 t)
					throws Exception {
				return new Tuple2<KeyType, RecordType1>(groupingKeyGenerator1.execute(t), t);
			}
		});
		
		/**	
		mapping basicCollection2 using groupingKeyGenerator2 to create key for every record.
		*/
		JavaPairRDD<KeyType, RecordType2> pair2 = distData2.mapToPair(new PairFunction<RecordType2, KeyType, RecordType2>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<KeyType, RecordType2> call(
					RecordType2 t)
					throws Exception {
				return new Tuple2<KeyType, RecordType2>(groupingKeyGenerator2.execute(t), t);
			}
		});
		
		/**
		 * 'cogroup' function from spark is used to join basicCollections, here output will be the pair of <key123, tuple<<all the records from basicCollection1 that belong to 'key123'>,<all the records from basicCollection2 that belong to 'key123'>>>
		 */
		JavaPairRDD<KeyType, Tuple2<Iterable<RecordType1>,Iterable<RecordType2>>> cogroup = pair1.cogroup(pair2);
		
		/**
		 * finally result of co-grouping the records is mapped to 'OutputrecordType' to create final result. 
		 */
		JavaRDD<OutputRecordType> outputRecord = cogroup.flatMap(new FlatMapFunction<Tuple2<KeyType, Tuple2<Iterable<RecordType1>,Iterable<RecordType2>>>, OutputRecordType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<OutputRecordType> call(
					Tuple2<KeyType, Tuple2<Iterable<RecordType1>,Iterable<RecordType2>>> t)
					throws Exception {
				
				ResultSetCollector<OutputRecordType> resultCollector = new ResultSetCollector<>();
				
				resultCollector.setResult(new ResultSet<OutputRecordType>());
				
				resultCollector.initialise();
				
				resultMapper.mapRecord(new Pair<Iterable<RecordType1>, Iterable<RecordType2>>(t._2()._1(), t._2()._2()), resultCollector);
								
				resultCollector.finalise();
				
				return resultCollector.getResult().get().iterator(); 
			}
		});
			
		result.setData(outputRecord);
		
		return result;
		
	}
	
/**
 * 'append' function is used to concatenate 2 RDDs.  
 */
	public <RecordType> ResultSet<RecordType> append(BasicCollection<RecordType> data1, BasicCollection<RecordType> data2) {
		SparkResultSet<RecordType> result = new SparkResultSet<>();
		
		JavaRDD<RecordType> distData1 = prepareBasicCollection(data1).getDistributedData();
		JavaRDD<RecordType> distData2 = prepareBasicCollection(data2).getDistributedData();
		
		/**
		 * 'union' function from spark is to append one RDD to another RDD.
		 */
		JavaRDD<RecordType> concatinatedResult = distData1.union(distData2);
		
		result.setData(concatinatedResult);
		
		return result;
		
	}

}
