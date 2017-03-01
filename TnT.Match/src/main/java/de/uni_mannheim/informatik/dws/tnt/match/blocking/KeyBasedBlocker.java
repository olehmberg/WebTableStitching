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
package de.uni_mannheim.informatik.dws.tnt.match.blocking;

import java.util.Arrays;
import java.util.HashSet;
import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.tnt.match.blocking.KeyBasedBlockingKeyGenerator.AmbiguityAvoidance;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.blocking.MultiBlockingKeyGenerator;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyBasedBlocker<RecordToKeyJoinKey, RecordType extends Matchable, SchemaElementType extends Matchable, KeyType> { // extends Blocker<RecordType, SchemaElementType> {

	//TODO cannot extend Blocker at the moment, as we need to return Correspondence instead of BlockedMatchable -> change BlockedMatchable into Correspondence in the definition of Blocker (contain the same data anyway) 
	
	private BasicCollection<KeyType> keys;
	private Function<RecordToKeyJoinKey, RecordType> recordToJoinKey; 
	private Function<RecordToKeyJoinKey, KeyType> keyToJoinKey;
	private KeyBasedBlockingKeyGenerator<RecordType, KeyType> blockingKeyGenerator;
	private ResultSet<Object> ambiguousValues;
	
	/**
	 * @param blockingFunction
	 */
	public KeyBasedBlocker(BasicCollection<KeyType> keys,
			Function<RecordToKeyJoinKey, RecordType> recordToJoinKey, 
			Function<RecordToKeyJoinKey, KeyType> keyToJoinKey,
			KeyBasedBlockingKeyGenerator<RecordType, KeyType> blockingKeyGenerator) {
		this.keys = keys;
		this.recordToJoinKey = recordToJoinKey;
		this.keyToJoinKey = keyToJoinKey;
		this.blockingKeyGenerator = blockingKeyGenerator;
	}

	protected void calculateAmbiguousValues(DataSet<RecordType, SchemaElementType> dataset, DataProcessingEngine engine) {
		
		//TODO this produces only a semi-global ambiguity avoidance
		// -- a value is only recognised as ambiguous if it occurs twice for the same record
		// -- a value which occurs in different attributes for different records is not detected
		// to get a global one, group all individual key values by their attributes
		// if any value has more than one key attribute, it's ambiguous
		
		ambiguousValues = engine.coGroup(dataset, keys, recordToJoinKey, keyToJoinKey, new RecordMapper<Pair<Iterable<RecordType>,Iterable<KeyType>>,Object>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Iterable<RecordType>, Iterable<KeyType>> record,
					DatasetIterator<Object> resultCollector) {
				
				// for each group, generate all key values as blocking keys
				
				// iterate over all keys
				for(KeyType key : record.getSecond()) {
					
					// iterate over all records
					for(RecordType r : record.getFirst()) {
						
						// count the frequency of all candidate key values
						Object[] keyValues = blockingKeyGenerator.getKeyValues(r, key);
						Distribution<Object> frequencies = Distribution.fromCollection(Arrays.asList(keyValues));
						
						
						// if a value occurred in more than one attribute of the current record's current candidate key
						for(Object value : frequencies.getElements()) {
							if(frequencies.getFrequency(value)>1) {
								// it's an ambiguous value
								resultCollector.next(value);
							}
						}
						
					}
					
				}
				
			}
		});
		
		engine.distinct(ambiguousValues);
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.StandardBlocker#runBlocking(de.uni_mannheim.informatik.wdi.model.DataSet, boolean, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)
	 */
//	@Override
	public ResultSet<Correspondence<RecordType, SchemaElementType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric,
			final ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			DataProcessingEngine engine) {

		if(blockingKeyGenerator.getAmbiguityAvoidanceMode()==AmbiguityAvoidance.Global) {
			calculateAmbiguousValues(dataset, engine);
			
			//TODO this should be solved with a broadcast variable
			blockingKeyGenerator.setAmbiguousValues(new HashSet<>(ambiguousValues.get()));
		}
		
		// coGroup records and candidate keys via table id
		// -> bring together the candidate keys and the records of the same table
		ResultSet<Pair<String, RecordType>> recordsWithBlockingKeys = engine.coGroup(dataset, keys, recordToJoinKey, keyToJoinKey, new RecordMapper<Pair<Iterable<RecordType>,Iterable<KeyType>>,Pair<String, RecordType>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Iterable<RecordType>, Iterable<KeyType>> record,
					DatasetIterator<Pair<String, RecordType>> resultCollector) {
				
				// for each group, generate all key values as blocking keys
				
				// iterate over all keys
				for(KeyType key : record.getSecond()) {
					
					// iterate over all records
					for(RecordType r : record.getFirst()) {
						
						// create the blocking key on record level
						String blockingKey = blockingKeyGenerator.getBlockingKey(r, key);
						
						if(blockingKey!=null) {
							resultCollector.next(new Pair<String, RecordType>(blockingKey, r));
						}
						
					}
					
				}
				
			}
		});
		
		// join the records based on their blocking keys to form pairs
		 ResultSet<Pair<Pair<String, RecordType>, Pair<String, RecordType>>> blockedPairs = engine.symmetricSelfJoin(recordsWithBlockingKeys, new Function<String,Pair<String, RecordType>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public String execute(Pair<String, RecordType> input) {
				return input.getFirst();
			}} );
		
		// make sure no two records from the same table were joined (i.e. two different keys have the same set of values)
		 blockedPairs = engine.filter(blockedPairs, new Function<Boolean, Pair<Pair<String, RecordType>, Pair<String, RecordType>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean execute(Pair<Pair<String, RecordType>, Pair<String, RecordType>> input) {
				return recordToJoinKey.execute(input.getFirst().getSecond()) != recordToJoinKey.execute(input.getSecond().getSecond());
			}
		});
		 
		// change the pair type to MatchingTask
		 ResultSet<Correspondence<RecordType, SchemaElementType>> blockedMatchables = engine.transform(blockedPairs, new RecordMapper<Pair<Pair<String, RecordType>, Pair<String, RecordType>>, Correspondence<RecordType, SchemaElementType>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Pair<String, RecordType>, Pair<String, RecordType>> record,
					DatasetIterator<Correspondence<RecordType, SchemaElementType>> resultCollector) {
				
				// we don't know in which order the rows were joined, but we want all correspondences to be in the same direction
				// so, the first row is the one with the lower table id (by convention)
				
				RecordType firstRow = record.getFirst().getSecond();
				RecordType secondRow = record.getSecond().getSecond();
				
				if(firstRow.getIdentifier().compareTo(secondRow.getIdentifier())>0) {
					RecordType tmp = secondRow;
					secondRow = firstRow;
					firstRow = tmp;
				}
				
				Correspondence<RecordType, SchemaElementType> matchingTask = new Correspondence<RecordType, SchemaElementType>(firstRow, secondRow, 0.0, schemaCorrespondences);
				
				resultCollector.next(matchingTask);
			}
		});
		 
		return blockedMatchables;
	}


}
