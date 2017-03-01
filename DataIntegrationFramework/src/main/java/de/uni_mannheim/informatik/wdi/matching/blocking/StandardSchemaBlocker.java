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
package de.uni_mannheim.informatik.wdi.matching.blocking;

import de.uni_mannheim.informatik.wdi.matching.MatchingTask;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.Function;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class StandardSchemaBlocker<SchemaElementType extends Matchable, RecordType extends Matchable> extends SchemaBlocker<SchemaElementType, RecordType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private BlockingKeyGenerator<SchemaElementType> keyGenerator;
	
	/**
	 * 
	 */
	public StandardSchemaBlocker(BlockingKeyGenerator<SchemaElementType> keyGenerator) {
		this.keyGenerator = keyGenerator;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker#initialise(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public void initialise(DataSet<SchemaElementType, SchemaElementType> schema1,
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker#initialise(de.uni_mannheim.informatik.wdi.model.DataSet, boolean, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public void initialise(DataSet<SchemaElementType, SchemaElementType> dataset, boolean isSymmetric,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker#runBlocking(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)
	 */
	@Override
	public ResultSet<BlockedMatchable<SchemaElementType, RecordType>> runBlocking(
			DataSet<SchemaElementType, SchemaElementType> schema1,
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			DataProcessingEngine engine) {
		ResultSet<BlockedMatchable<SchemaElementType, RecordType>> result = new ResultSet<>();
		
		Function<String, SchemaElementType> joinKeyGenerator1 = new Function<String, SchemaElementType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(SchemaElementType input) {
				return keyGenerator.getBlockingKey(input);
			}
		};
		
		Function<String, SchemaElementType> joinKeyGenerator2 = new Function<String, SchemaElementType>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(SchemaElementType input) {
				return keyGenerator.getBlockingKey(input);
			}
		};
		
		for(Pair<SchemaElementType, SchemaElementType> p : engine.join(schema1, schema2, joinKeyGenerator1, joinKeyGenerator2).get()) {
			result.add(new MatchingTask<SchemaElementType, RecordType>(p.getFirst(), p.getSecond(), instanceCorrespondences));
		}


		calculatePerformance(schema1, schema2, result);
		
		return result;
	}

}
