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

import java.util.HashMap;
import java.util.Map;

import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class AggregateCollector<KeyType, RecordType, ResultType> extends GroupCollector<KeyType, RecordType>  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<KeyType, ResultType> intermediateResults;
	private ResultSet<Pair<KeyType, ResultType>> aggregationResult;
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.GroupCollector#initialise()
	 */
	@Override
	public void initialise() {
		// TODO Auto-generated method stub
		super.initialise();
		intermediateResults = new HashMap<>();
		aggregationResult = new ResultSet<>();
	}
	
	/**
	 * @param aggregationResult the aggregationResult to set
	 */
	protected void setAggregationResult(
			ResultSet<Pair<KeyType, ResultType>> aggregationResult) {
		this.aggregationResult = aggregationResult;
	}
	/**
	 * @return the aggregationResult
	 */
	public ResultSet<Pair<KeyType, ResultType>> getAggregationResult() {
		return aggregationResult;
	}

	private DataAggregator<KeyType, RecordType, ResultType> aggregator;
	/**
	 * @param aggregator the aggregator to set
	 */
	public void setAggregator(DataAggregator<KeyType, RecordType, ResultType> aggregator) {
		this.aggregator = aggregator;
	}
	
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.GroupCollector#next(de.uni_mannheim.informatik.wdi.model.Pair)
	 */
	@Override
	public void next(Pair<KeyType, RecordType> record) {
		ResultType result = intermediateResults.get(record.getFirst());
		
		if(result==null) {
			result = aggregator.initialise(record.getFirst());
		}
		
		result = aggregator.aggregate(result, record.getSecond());
		intermediateResults.put(record.getFirst(), result);
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.GroupCollector#finalise()
	 */
	@Override
	public void finalise() {
		for(KeyType key : intermediateResults.keySet()) {
			aggregationResult.add(new Pair<>(key, intermediateResults.get(key)));
		}
	}
}
