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
package de.uni_mannheim.informatik.wdi.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.time.DurationFormatUtils;

import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Consumer;
import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Parallel;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.AggregateCollector;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.GroupCollector;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;
import de.uni_mannheim.informatik.wdi.processing.ResultSetCollector;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ParallelDataProcessingEngine extends DataProcessingEngine {

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#createResultSet(java.lang.Object)
	 */
	@Override
	public <RecordType> ResultSet<RecordType> createResultSet(RecordType dummyForTypeInference) {
		return new ThreadSafeResultSet<>();
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#iterateDataset(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.processing.DatasetIterator)
	 */
	@Override
	public <RecordType> void iterateDataset(BasicCollection<RecordType> dataset,
			final DatasetIterator<RecordType> iterator) {
		iterator.initialise();
		
		new Parallel<RecordType>().tryForeach(dataset.get(), new Consumer<RecordType>() {

			@Override
			public void execute(RecordType parameter) {
				iterator.next(parameter);
			}
		});
		iterator.finalise();
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#transform(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.RecordMapper)
	 */
	@Override
	public <RecordType, OutputRecordType> ResultSet<OutputRecordType> transform(BasicCollection<RecordType> dataset,
			final RecordMapper<RecordType, OutputRecordType> transformation) {
		final ResultSetCollector<OutputRecordType> resultCollector = new ResultSetCollector<>();
		
		// the worst line of code ever ... is there a better way to do that in java?
		resultCollector.setResult(createResultSet((OutputRecordType)null));
		
		resultCollector.initialise();
		
		new Parallel<RecordType>().tryForeach(dataset.get(), new Consumer<RecordType>() {

			@Override
			public void execute(RecordType parameter) {
				transformation.mapRecord(parameter, resultCollector);
			}
			
		});
		
		resultCollector.finalise();
		
		return resultCollector.getResult();
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#groupRecords(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.RecordMapper)
	 */
	@Override
	public <KeyType, RecordType, OutputRecordType> ResultSet<Group<KeyType, OutputRecordType>> groupRecords(
			BasicCollection<RecordType> dataset,
			final RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy) {
		
		final GroupCollector<KeyType, OutputRecordType> groupCollector = new ThreadSafeGroupCollector<>();
		
		groupCollector.initialise();
		
		if(dataset!=null) {
			new Parallel<RecordType>().tryForeach(dataset.get(), new Consumer<RecordType>() {
	
				@Override
				public void execute(RecordType parameter) {
					groupBy.mapRecord(parameter, groupCollector);
				}
			});
		}
		
		groupCollector.finalise();
		
		return groupCollector.getResult();
	}
	
	public <KeyType, RecordType, OutputRecordType, ResultType> ResultSet<Pair<KeyType, ResultType>> aggregateRecords(
			BasicCollection<RecordType> dataset, 
			final RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy, 
			DataAggregator<KeyType, OutputRecordType, ResultType> aggregator) {

		final AggregateCollector<KeyType, OutputRecordType, ResultType> aggregateCollector = new ThreadSafeAggregateCollector<>();
		
		aggregateCollector.setAggregator(aggregator);
		aggregateCollector.initialise();
		
		new Parallel<RecordType>().tryForeach(dataset.get(), new Consumer<RecordType>() {

			@Override
			public void execute(RecordType parameter) {
				groupBy.mapRecord(parameter, aggregateCollector);
			}
		});
		
		aggregateCollector.finalise();
		
		return aggregateCollector.getAggregationResult();
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#hashRecords(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.Function)
	 */
	@Override
	protected <KeyType, RecordType> Map<KeyType, List<RecordType>> hashRecords(BasicCollection<RecordType> dataset,
			final Function<KeyType, RecordType> hash) {
		final ConcurrentHashMap<KeyType, List<RecordType>> hashMap = new ConcurrentHashMap<>(dataset.size());
		
		new Parallel<RecordType>().tryForeach(dataset.get(), new Consumer<RecordType>() {

			@Override
			public void execute(RecordType record) {
				KeyType key = hash.execute(record);
				
				if(key!=null) {
					hashMap.putIfAbsent(key, Collections.synchronizedList(new LinkedList<RecordType>()));
					
					List<RecordType> records = hashMap.get(key);
					
					records.add(record);
				}
			}
		});
		
		for(KeyType key : hashMap.keySet()) {
			hashMap.put(key, new ArrayList<>(hashMap.get(key)));
		}
		
		return hashMap;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#join(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.Function, de.uni_mannheim.informatik.wdi.processing.Function)
	 */
	@Override
	public <KeyType, RecordType> ResultSet<Pair<RecordType, RecordType>> join(
			BasicCollection<RecordType> dataset1,
			BasicCollection<RecordType> dataset2, 
			final Function<KeyType, RecordType> joinKeyGenerator1,
			final Function<KeyType, RecordType> joinKeyGenerator2) {
		final ResultSet<Pair<RecordType, RecordType>> result = createResultSet((Pair<RecordType, RecordType>)null);
		
		final Map<KeyType, List<RecordType>> joinKeys1 = hashRecords(dataset1, joinKeyGenerator1);
		final Map<KeyType, List<RecordType>> joinKeys2 = hashRecords(dataset2, joinKeyGenerator2);

		// calculate the result
		new Parallel<KeyType>().tryForeach(joinKeys1.keySet(), new Consumer<KeyType>() {

			@Override
			public void execute(KeyType key1) {
				Collection<RecordType> block = joinKeys1.get(key1);
				Collection<RecordType> block2 = joinKeys2.get(key1);
				
				if(block2!=null) {
					
					for(RecordType r1 : block) {
						for(RecordType r2 : block2) {
							result.add(new Pair<>(r1, r2));
						}
					}
					
				}
			}
			
		});
		
		return result;
	}
	
//	public <KeyType, RecordType1, RecordType2> ResultSet<Pair<RecordType1,RecordType2>> joinMixedTypes(BasicCollection<RecordType1> dataset1, BasicCollection<RecordType2> dataset2, Function<KeyType, RecordType1> joinKeyGenerator1, Function<KeyType, RecordType2> joinKeyGenerator2) {
//		
//		final ResultSet<Pair<RecordType1, RecordType2>> result = createResultSet((Pair<RecordType1, RecordType2>)null);
//		
//		final Map<KeyType, List<RecordType1>> joinKeys1 = hashRecords(dataset1, joinKeyGenerator1);
//		final Map<KeyType, List<RecordType2>> joinKeys2 = hashRecords(dataset2, joinKeyGenerator2);
//		
//		// calculate the result
//		new Parallel<KeyType>().tryForeach(joinKeys1.keySet(), new Consumer<KeyType>() {
//
//			@Override
//			public void execute(KeyType key1) {
//				List<RecordType1> block = joinKeys1.get(key1);
//				List<RecordType2> block2 = joinKeys2.get(key1);
//				
//				if(block2!=null) {
//					
//					for(RecordType1 r1 : block) {
//						for(RecordType2 r2 : block2) {
//							result.add(new Pair<>(r1, r2));
//						}
//					}
//					
//				}
//			}
//			
//		});
//		
//		return result;
//	}
	
	@Override
	public <KeyType, RecordType> ResultSet<Pair<RecordType,RecordType>> symmetricSelfJoin(
			BasicCollection<RecordType> dataset, 
			final Function<KeyType, RecordType> joinKeyGenerator,
			final ResultSetCollector<Pair<RecordType, RecordType>> collector) {
		
		final Map<KeyType, List<RecordType>> joinKeys = hashRecords(dataset, joinKeyGenerator);
		
//		for(KeyType key : joinKeys.keySet()) {
//			System.out.println(key.toString());
//			for(RecordType record : joinKeys.get(key)) {
//				System.out.println(String.format("\t%s", record));
//			}
//		}
		
		System.out.println(String.format("[ParallelDataProcessingEngine] symmetricSelfJoin: %d join key values", joinKeys.size()));
		
		collector.setResult(createResultSet((Pair<RecordType, RecordType>)null));
		collector.initialise();
		
		List<Pair<List<RecordType>, Integer[]>> tasks = new LinkedList<>();
		int idx = 0;
		for(List<RecordType> block : Q.sort(joinKeys.values(), new Comparator<List<RecordType>>() {

			@Override
			public int compare(List<RecordType> o1, List<RecordType> o2) {
				return Integer.compare(o1.size(), o2.size());
			}
		})) {
			
			if(idx++==joinKeys.values().size()-1) {
			
				// split the largest hash bucket into smaller parts, such that it can be distributed among more processors
				// in cases where few very large hash buckets exists (fewer than number of processors), they will take quite long to process and all other processors will be idle during that time
				// so we split the largest bucket to make sure all processors are busy
				
//				for(int i = 0; i < block.size(); i++) {
//					Pair<List<RecordType>, Integer[]> task = new Pair<List<RecordType>, Integer[]>(block, new Integer[] { i });
//					tasks.add(task);
//				}
				
				int startIndex = 0;
				
				if(block.size()%2!=0) {
					Pair<List<RecordType>, Integer[]> task = new Pair<List<RecordType>, Integer[]>(block, new Integer[] { 0 });
					tasks.add(task);
					startIndex++;
				}
				
//				for(int i = startIndex; i < block.size()/2 - 1; i++) {
				for(int i = startIndex; i < block.size()/2; i++) {
					Pair<List<RecordType>, Integer[]> task = new Pair<List<RecordType>, Integer[]>(block, new Integer[] { i, block.size() - i - 1 + startIndex });
					tasks.add(task);
				}
			
			} else {				
				Pair<List<RecordType>, Integer[]> task = new Pair<List<RecordType>, Integer[]>(block, null);
				tasks.add(task);
				
			}
		}
		
		System.out.println(String.format("[ParallelDataProcessingEngine] symmetricSelfJoin: %d tasks", tasks.size()));
		
		long start = System.currentTimeMillis();
		
		new Parallel<Pair<List<RecordType>, Integer[]>>().tryForeach(tasks, new Consumer<Pair<List<RecordType>, Integer[]>>() {

			@Override
			public void execute(Pair<List<RecordType>, Integer[]> task) {
				
				if(task.getSecond()!=null) {
					for(int i : task.getSecond()) {
						for(int j = i+1; j<task.getFirst().size(); j++) {
							if(i!=j) {
								collector.next(new Pair<>(task.getFirst().get(i), task.getFirst().get(j)));
							}
						}
					}
				} else {
					for(int i = 0; i < task.getFirst().size(); i++) {
						for(int j = i+1; j<task.getFirst().size(); j++) {
							if(i!=j) {
								collector.next(new Pair<>(task.getFirst().get(i), task.getFirst().get(j)));
							}
						}
					}					
				}
				
			}
		});
		
		System.out.println(String.format("[ParallelDataProcessingEngine] symmetricSelfJoin: %s", DurationFormatUtils.formatDurationHMS(System.currentTimeMillis()-start)));
		
//		new Parallel<List<RecordType>>().tryForeach(joinKeys.values(), new Consumer<List<RecordType>>() {
//
//			@Override
//			public void execute(List<RecordType> block) {
//				for(int i = 0; i < block.size(); i++) {
//					for(int j = i+1; j<block.size(); j++) {
//						if(i!=j) {
//							collector.next(new Pair<>(block.get(i), block.get(j)));
//						}
//					}
//				}
//			}
//		});
		
		collector.finalise();
		
		return collector.getResult();
	}
}
