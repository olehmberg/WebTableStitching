package de.uni_mannheim.informatik.wdi.matching.blocking;

import de.uni_mannheim.informatik.wdi.matching.blocking.recordmappers.JoinedGroupsToMatchingTaskMapper;
import de.uni_mannheim.informatik.wdi.matching.blocking.recordmappers.MultiBlockingKeyRecordMapper;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.GroupJoinKeyGenerator;

public class MultiKeyBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends Blocker<RecordType, SchemaElementType>{

	private MultiBlockingKeyGenerator<RecordType> blockingFunction;
	private MultiBlockingKeyGenerator<RecordType> secondBlockingFunction;
  
	public MultiKeyBlocker() {
		
	}
	
	public MultiKeyBlocker(MultiBlockingKeyGenerator<RecordType> blockingFunction) {
		this.blockingFunction = blockingFunction;
		this.secondBlockingFunction = blockingFunction;
	}
	
	/**
	 * 
	 * Creates a new Multi Key Blocker with the given blocking function(s). 
	 * If two datasets are used and secondBlockingFunction is not null, secondBlockingFunction will be used for the second dataset. If it is null, blockingFunction will be used for both datasets 
	 * 
	 * @param blockingFunction
	 * @param secondBlockingFunction
	 */
	public MultiKeyBlocker(MultiBlockingKeyGenerator<RecordType> blockingFunction, MultiBlockingKeyGenerator<RecordType> secondBlockingFunction) {
		this.blockingFunction = blockingFunction;
		this.secondBlockingFunction = secondBlockingFunction == null ? blockingFunction : secondBlockingFunction;
	}
	
	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset1,
			DataSet<RecordType, SchemaElementType> dataset2,
			final ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ResultSet<>();
		
		MultiBlockingKeyRecordMapper<RecordType> mapper1 = new MultiBlockingKeyRecordMapper<>(blockingFunction);
		ResultSet<Group<String, RecordType>> grouped1 = engine.groupRecords(dataset1, mapper1);
		
//		ResultSet<Group<String, RecordType>> grouped1 = engine.groupRecords(dataset1, new RecordKeyValueMapper<String, RecordType, RecordType>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(RecordType record,
//					DatasetIterator<Pair<String, RecordType>> resultCollector) {
//				
//				Collection<String> candidates = blockingFunction.getMultiBlockingKey(record);
//				
//				for(String candidate : candidates) {
//					resultCollector.next(new Pair<String, RecordType>(candidate, record));
//				}
//			}
//		});
//		
		
		MultiBlockingKeyRecordMapper<RecordType> mapper2 = new MultiBlockingKeyRecordMapper<>(secondBlockingFunction);
		ResultSet<Group<String, RecordType>> grouped2 = engine.groupRecords(dataset2, mapper2); 
		
//		ResultSet<Group<String, RecordType>> grouped2 = engine.groupRecords(dataset2, new RecordKeyValueMapper<String, RecordType, RecordType>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(RecordType record,
//					DatasetIterator<Pair<String, RecordType>> resultCollector) {
//				
//				Collection<String> candidates = secondBlockingFunction.getMultiBlockingKey(record);
//				
//				for(String candidate : candidates) {
//					resultCollector.next(new Pair<String, RecordType>(candidate, record));
//				}
//			}
//		});
		
//		Function<String, Group<String, RecordType>> joinKeyGenerator = new Function<String, Group<String, RecordType>>() {
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(Group<String, RecordType> input) {
//				return input.getKey();
//			}
//		};		

		Function<String, Group<String, RecordType>> joinKeyGenerator = new GroupJoinKeyGenerator<>();
		
		ResultSet<Pair<Group<String,RecordType>,Group<String,RecordType>>> blockedData = engine.join(grouped1, grouped2, joinKeyGenerator);
		
		JoinedGroupsToMatchingTaskMapper<String, RecordType, SchemaElementType> groupsToMatchingTaskMapper = new JoinedGroupsToMatchingTaskMapper<>(schemaCorrespondences);
		
		result = engine.transform(blockedData, groupsToMatchingTaskMapper);
		
//		result = engine.transform(blockedData, new RecordMapper<Pair<Group<String,RecordType>,Group<String,RecordType>>, BlockedMatchable<RecordType, SchemaElementType>>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(
//					Pair<Group<String, RecordType>, Group<String, RecordType>> record,
//					DatasetIterator<BlockedMatchable<RecordType, SchemaElementType>> resultCollector) {
//				for(RecordType web : record.getFirst().getRecords().get()){
//					for(RecordType kb : record.getSecond().getRecords().get()){
//						resultCollector.next(new MatchingTask<RecordType, SchemaElementType>(web, kb, schemaCorrespondences));	
//					}
//				}
//			}
//		});

		calculatePerformance(dataset1, dataset2, result);
		
		return result;
	}

//	TODO later
	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			DataProcessingEngine engine) {
		// TODO Auto-generated method stub
		return null;
	}

}
