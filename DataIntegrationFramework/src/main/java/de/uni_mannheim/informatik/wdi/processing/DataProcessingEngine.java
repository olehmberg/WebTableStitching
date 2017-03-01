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
package de.uni_mannheim.informatik.wdi.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DataProcessingEngine {
	
	public <RecordType> ResultSet<RecordType> createResultSet(RecordType dummyForTypeInference) {
		return new ResultSet<>();
	}
	
	public <RecordType extends Matchable, SchemaElementType> DataSet<RecordType, SchemaElementType> createDataSet(RecordType dummyForTypeInference, SchemaElementType secondDummy) {
		return new DefaultDataSet<>();
	}
	
	public <RecordType> ResultSet<RecordType> assignUniqueRecordIds(ResultSet<RecordType> data, Function<RecordType, Pair<Long,RecordType>> assignUniqueId) {
		long id = 0;
		
		ResultSet<RecordType> result = createResultSet((RecordType)null);
		
		for(RecordType record : data.get()) {
			RecordType r = assignUniqueId.execute(new Pair<Long, RecordType>(id++, record));
			result.add(r);
		}
		
		return result;
	}
	
	public <RecordType extends Matchable, SchemaElementType> DataSet<RecordType, SchemaElementType> assignUniqueRecordIds(DataSet<RecordType, SchemaElementType> data, Function<RecordType, Pair<Long,RecordType>> assignUniqueId) {
		long id = 0;
		
		DataSet<RecordType, SchemaElementType> result = createDataSet((RecordType)null, (SchemaElementType)null);
		
		for(RecordType record : data.get()) {
			RecordType r = assignUniqueId.execute(new Pair<Long, RecordType>(id++, record));
			result.add(r);
		}
		
		return result;
	}
	
	public <RecordType> void iterateDataset(BasicCollection<RecordType> dataset, DatasetIterator<RecordType> iterator) {
		
		iterator.initialise();
		
		for(RecordType r : dataset.get()) {
			iterator.next(r);
		}
		
		iterator.finalise();
	}
	
	public <InputType, RecordType extends Matchable, SchemaElementType> DataSet<RecordType, SchemaElementType> createDataset(BasicCollection<InputType> input, RecordMapper<InputType, RecordType> transformation) {
		
		DataSetCollector<RecordType, SchemaElementType> resultCollector = new DataSetCollector<>();
		
		// the worst line of code ever ... is there a better way to do that in java?
		resultCollector.setResult(createDataSet((RecordType)null, (SchemaElementType)null));
		
		resultCollector.initialise();
		
		for(InputType record : input.get()) {
			transformation.mapRecord(record, resultCollector);
		}
		
		resultCollector.finalise();
		
		return resultCollector.getResult();
	}
	
	public <RecordType, OutputRecordType> ResultSet<OutputRecordType> transform(BasicCollection<RecordType> dataset, RecordMapper<RecordType, OutputRecordType> transformation) {
		
		ResultSetCollector<OutputRecordType> resultCollector = new ResultSetCollector<>();
		
		// the worst line of code ever ... is there a better way to do that in java?
		resultCollector.setResult(createResultSet((OutputRecordType)null));
		
		resultCollector.initialise();
		
		for(RecordType record : dataset.get()) {
			transformation.mapRecord(record, resultCollector);
		}
		
		resultCollector.finalise();
		
		return resultCollector.getResult();
	}
	
	protected <KeyType, RecordType> Map<KeyType, List<RecordType>> hashRecords(BasicCollection<RecordType> dataset, Function<KeyType, RecordType> hash) {
		HashMap<KeyType, List<RecordType>> hashMap = new HashMap<>();
		
		for(RecordType record : dataset.get()) {
			KeyType key = hash.execute(record);
			
			if(key!=null) {
				List<RecordType> records = hashMap.get(key);
				if(records==null) {
					records = new ArrayList<>();
					hashMap.put(key, records);
				}
				
				records.add(record);
			}
		}
		
		return hashMap;
	}

	public <KeyType, RecordType> ResultSet<Pair<RecordType,RecordType>> symmetricSelfJoin(BasicCollection<RecordType> dataset, Function<KeyType, RecordType> joinKeyGenerator) {
		return symmetricSelfJoin(dataset, joinKeyGenerator, new ResultSetCollector<Pair<RecordType, RecordType>>());
	}
	
	public <KeyType, RecordType> ResultSet<Pair<RecordType,RecordType>> symmetricSelfJoin(BasicCollection<RecordType> dataset, Function<KeyType, RecordType> joinKeyGenerator, final ResultSetCollector<Pair<RecordType, RecordType>> collector) {
		
//		ResultSet<Pair<RecordType, RecordType>> result = createResultSet((Pair<RecordType, RecordType>)null);
		
		Map<KeyType, List<RecordType>> joinKeys = hashRecords(dataset, joinKeyGenerator);
		
//		for(RecordType record : dataset.get()) {
//			KeyType key = joinKeyGenerator.execute(record);
//			
//			if(key!=null) {
//				List<RecordType> records = joinKeys.get(key);
//				if(records==null) {
//					records = new ArrayList<>();
//					joinKeys.put(key, records);
//				}
//				
//				records.add(record);
//			}
//		}
		
		collector.setResult(createResultSet((Pair<RecordType,RecordType>)null));
		collector.initialise();
		
		for(List<RecordType> block : joinKeys.values()) {
			for(int i = 0; i < block.size(); i++) {
				for(int j = i+1; j<block.size(); j++) {
					if(i!=j) {
						collector.next(new Pair<>(block.get(i), block.get(j)));
					}
				}
			}
		}
		
		collector.finalise();
		
		return collector.getResult();
	}
	
	public <KeyType, RecordType> ResultSet<Pair<RecordType,RecordType>> join(BasicCollection<RecordType> dataset1, BasicCollection<RecordType> dataset2, Function<KeyType, RecordType> joinKeyGenerator) {
		
		return join(dataset1, dataset2, joinKeyGenerator, joinKeyGenerator);
		
	}
	
	public <KeyType, RecordType> ResultSet<Pair<RecordType,RecordType>> join(BasicCollection<RecordType> dataset1, BasicCollection<RecordType> dataset2, Function<KeyType, RecordType> joinKeyGenerator1, Function<KeyType, RecordType> joinKeyGenerator2) {
		
		ResultSet<Pair<RecordType, RecordType>> result = createResultSet((Pair<RecordType, RecordType>)null);
		
		Map<KeyType, List<RecordType>> joinKeys1 = hashRecords(dataset1, joinKeyGenerator1);
		Map<KeyType, List<RecordType>> joinKeys2 = hashRecords(dataset2, joinKeyGenerator2);
		
//		for(RecordType record : dataset1.get()) {
//			KeyType key = joinKeyGenerator1.execute(record);
//			
//			List<RecordType> records = joinKeys1.get(key);
//			if(records==null) {
//				records = new ArrayList<>();
//				joinKeys1.put(key, records);
//			}
//			
//			records.add(record);
//		}
//		
//		for(RecordType record : dataset2.get()) {
//			KeyType key = joinKeyGenerator2.execute(record);
//			
//			List<RecordType> records = joinKeys2.get(key);
//			if(records==null) {
//				records = new ArrayList<>();
//				joinKeys2.put(key, records);
//			}
//			
//			records.add(record);
//		}
		
		for(KeyType key1 : joinKeys1.keySet()) {
			List<RecordType> block = joinKeys1.get(key1);
			List<RecordType> block2 = joinKeys2.get(key1);
			
			if(block2!=null) {
				
				for(RecordType r1 : block) {
					for(RecordType r2 : block2) {
						result.add(new Pair<>(r1, r2));
					}
				}
				
			}
			
		}
		
		return result;
	}
	
	public <KeyType, RecordType1, RecordType2> ResultSet<Pair<RecordType1,RecordType2>> joinMixedTypes(BasicCollection<RecordType1> dataset1, BasicCollection<RecordType2> dataset2, Function<KeyType, RecordType1> joinKeyGenerator1, Function<KeyType, RecordType2> joinKeyGenerator2) {
		
		final Map<KeyType, List<RecordType1>> joinKeys1 = hashRecords(dataset1, joinKeyGenerator1);
		final Map<KeyType, List<RecordType2>> joinKeys2 = hashRecords(dataset2, joinKeyGenerator2);
		
		
		ResultSet<Pair<RecordType1, RecordType2>> result = transform(new ResultSet<KeyType>(joinKeys1.keySet()), new RecordMapper<KeyType, Pair<RecordType1, RecordType2>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(KeyType key1, DatasetIterator<Pair<RecordType1, RecordType2>> resultCollector) {
				List<RecordType1> block = joinKeys1.get(key1);
				List<RecordType2> block2 = joinKeys2.get(key1);
				
				if(block2!=null) {
					
					for(RecordType1 r1 : block) {
						for(RecordType2 r2 : block2) {
							resultCollector.next(new Pair<>(r1, r2));
						}
					}
					
				}
			}
		});
		
//		ResultSet<Pair<RecordType1, RecordType2>> result = createResultSet((Pair<RecordType1, RecordType2>)null);
//		
//		for(KeyType key1 : joinKeys1.keySet()) {
//			List<RecordType1> block = joinKeys1.get(key1);
//			List<RecordType2> block2 = joinKeys2.get(key1);
//			
//			if(block2!=null) {
//				
//				for(RecordType1 r1 : block) {
//					for(RecordType2 r2 : block2) {
//						result.add(new Pair<>(r1, r2));
//					}
//				}
//				
//			}
//			
//		}
		
		System.out.println(String.format("[DataProcessingEngine] joinMixedTypes: %d resulting records", result.size()));
		return result;
	}
	
	public <KeyType, RecordType1, RecordType2> ResultSet<Pair<RecordType1,RecordType2>> leftJoin(BasicCollection<RecordType1> dataset1, BasicCollection<RecordType2> dataset2, Function<KeyType, RecordType1> joinKeyGenerator1, Function<KeyType, RecordType2> joinKeyGenerator2) {
		
		ResultSet<Pair<RecordType1, RecordType2>> result = createResultSet((Pair<RecordType1, RecordType2>)null);
		
		Map<KeyType, List<RecordType1>> joinKeys1 = hashRecords(dataset1, joinKeyGenerator1);
		Map<KeyType, List<RecordType2>> joinKeys2 = hashRecords(dataset2, joinKeyGenerator2);
		
//		for(RecordType1 record : dataset1.get()) {
//			KeyType key = joinKeyGenerator1.execute(record);
//			
//			List<RecordType1> records = joinKeys1.get(key);
//			if(records==null) {
//				records = new ArrayList<>();
//				joinKeys1.put(key, records);
//			}
//			
//			records.add(record);
//		}
//		
//		for(RecordType2 record : dataset2.get()) {
//			KeyType key = joinKeyGenerator2.execute(record);
//			
//			List<RecordType2> records = joinKeys2.get(key);
//			if(records==null) {
//				records = new ArrayList<>();
//				joinKeys2.put(key, records);
//			}
//			
//			records.add(record);
//		}
		
		for(KeyType key1 : joinKeys1.keySet()) {
			List<RecordType1> block = joinKeys1.get(key1);
			List<RecordType2> block2 = joinKeys2.get(key1);
			
			for(RecordType1 r1 : block) {
				
				if(block2!=null) {
					for(RecordType2 r2 : block2) {
						result.add(new Pair<>(r1, r2));
					}
				} else {
					result.add(new Pair<>(r1, (RecordType2)null));
				}
			}
			
		}
		
		return result;
	}
	
	/***
	 * 
	 * Groups records based on the given groupBy mapper
	 * 
	 * KeyType = Type of the grouping key
	 * RecordType = Type of the input records
	 * OutputType = Type of the records in the resulting groups (can be the same as RecordType)
	 * 
	 * @param dataset
	 * @param groupBy
	 * @return
	 */
	public <KeyType, RecordType, OutputRecordType> ResultSet<Group<KeyType, OutputRecordType>> groupRecords(BasicCollection<RecordType> dataset, RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy) {
		
		GroupCollector<KeyType, OutputRecordType> groupCollector = new GroupCollector<>();
		
		groupCollector.initialise();
		
		for(RecordType r : dataset.get()) {
			groupBy.mapRecord(r, groupCollector);
		}
		
		groupCollector.finalise();
		
		return groupCollector.getResult();
	}
	
	public <KeyType, RecordType, OutputRecordType, ResultType> ResultSet<Pair<KeyType, ResultType>> aggregateRecords(BasicCollection<RecordType> dataset, RecordKeyValueMapper<KeyType, RecordType, OutputRecordType> groupBy, DataAggregator<KeyType, OutputRecordType, ResultType> aggregator) {

		AggregateCollector<KeyType, OutputRecordType, ResultType> aggregateCollector = new AggregateCollector<>();
		
		aggregateCollector.setAggregator(aggregator);
		aggregateCollector.initialise();
		
		for(RecordType r : dataset.get()) {
			groupBy.mapRecord(r, aggregateCollector);
		}
		
		aggregateCollector.finalise();
		
		return aggregateCollector.getAggregationResult();
	}

	public <ElementType, KeyType extends Comparable<KeyType>> ResultSet<ElementType> sort(BasicCollection<ElementType> data, final Function<KeyType, ElementType> sortingKey) {
		return sort(data, sortingKey, true);
	}
	
	public <ElementType, KeyType extends Comparable<KeyType>> ResultSet<ElementType> sort(BasicCollection<ElementType> data, final Function<KeyType, ElementType> sortingKey, final boolean ascending) {
		ArrayList<ElementType> list = new ArrayList<>(data.get());
		
		Collections.sort(list, new Comparator<ElementType>() {

			@Override
			public int compare(ElementType o1, ElementType o2) {
				return (ascending ? 1 : -1) * sortingKey.execute(o1).compareTo(sortingKey.execute(o2));
			}
		});
		
		ResultSet<ElementType> result = new ResultSet<>();
		for(ElementType elem : list) {
			result.add(elem);
		}
		
		return result;
	}
	
	/**
	 * Filters the given data. Only keeps element where criteria evaluates to true.
	 * @param data
	 * @param criteria
	 * @return
	 */
	public <ElementType> ResultSet<ElementType> filter(BasicCollection<ElementType> data, Function<Boolean, ElementType> criteria) {
		ResultSet<ElementType> result = new ResultSet<>();
		
		for(ElementType element : data.get()) {
			if(criteria.execute(element)) {
				result.add(element);
			}
		}
		
		return result;
	}
	
	public <KeyType, RecordType1, RecordType2, OutputRecordType> ResultSet<OutputRecordType> coGroup(
			BasicCollection<RecordType1> data1, 
			BasicCollection<RecordType2> data2, 
			final Function<KeyType, RecordType1> groupingKeyGenerator1, 
			final Function<KeyType, RecordType2> groupingKeyGenerator2, 
			final RecordMapper<Pair<Iterable<RecordType1>, Iterable<RecordType2>>, OutputRecordType> resultMapper) {
		 ResultSet<Group<KeyType, RecordType1>> group1 = groupRecords(data1, new RecordKeyValueMapper<KeyType, RecordType1, RecordType1>() {

			private static final long serialVersionUID = 1L;
		
			@Override
			public void mapRecord(RecordType1 record, DatasetIterator<Pair<KeyType, RecordType1>> resultCollector) {
				resultCollector.next(new Pair<KeyType, RecordType1>(groupingKeyGenerator1.execute(record), record));
			}
		});
		
		 ResultSet<Group<KeyType, RecordType2>> group2 = groupRecords(data2, new RecordKeyValueMapper<KeyType, RecordType2, RecordType2>() {

			private static final long serialVersionUID = 1L;
		
			@Override
			public void mapRecord(RecordType2 record, DatasetIterator<Pair<KeyType, RecordType2>> resultCollector) {
				resultCollector.next(new Pair<KeyType, RecordType2>(groupingKeyGenerator2.execute(record), record));
			}
		});
		 
		 ResultSet<Pair<Group<KeyType, RecordType1>, Group<KeyType, RecordType2>>> joined = joinMixedTypes(group1, group2, new Function<KeyType, Group<KeyType, RecordType1>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public KeyType execute(Group<KeyType, RecordType1> input) {
				return input.getKey();
			}
		},new Function<KeyType, Group<KeyType, RecordType2>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public KeyType execute(Group<KeyType, RecordType2> input) {
				return (KeyType)input.getKey();
			}
		});
		
		 return transform(joined, new RecordMapper<Pair<Group<KeyType, RecordType1>, Group<KeyType, RecordType2>>, OutputRecordType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Group<KeyType, RecordType1>, Group<KeyType, RecordType2>> record,
					DatasetIterator<OutputRecordType> resultCollector) {
				resultMapper.mapRecord(new Pair<Iterable<RecordType1>, Iterable<RecordType2>>(record.getFirst().getRecords().get(), record.getSecond().getRecords().get()), resultCollector);
			}
		});
	}
	
	public <RecordType> ResultSet<RecordType> append(BasicCollection<RecordType> data1, BasicCollection<RecordType> data2) {
		ResultSet<RecordType> result = new ResultSet<RecordType>();
		
		for(RecordType r : data1.get()) {
			result.add(r);
		}
		
		for(RecordType r : data2.get()) {
			result.add(r);
		}

		return result;
	}
	
	public <RecordType> ResultSet<RecordType> distinct(BasicCollection<RecordType> data) {
		Set<RecordType> set = new HashSet<>(data.get());
		
		ResultSet<RecordType> result = new ResultSet<>();
		
		for(RecordType record : set) {
			result.add(record);
		}
		
		return result;
	}
}
