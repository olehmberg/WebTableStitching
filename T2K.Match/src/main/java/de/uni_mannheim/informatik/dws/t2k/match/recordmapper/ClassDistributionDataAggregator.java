package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import java.util.HashMap;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;

public class ClassDistributionDataAggregator implements DataAggregator<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>, HashMap<String, Double>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Map<Integer, String> classIndices;
	
	public ClassDistributionDataAggregator(){
		
	}
	
	public ClassDistributionDataAggregator(Map<Integer, String> classIndices) {
		this.classIndices = classIndices;
	}



	@Override
	public HashMap<String, Double> aggregate(
			HashMap<String, Double> previousResult,
			Correspondence<MatchableTableRow, MatchableTableColumn> record) {
		if(previousResult != null){
			if(previousResult.containsKey(classIndices.get(record.getSecondRecord().getTableId())))
				previousResult.put(classIndices.get(record.getSecondRecord().getTableId()), previousResult.get((classIndices.get(record.getSecondRecord().getTableId())))+1.0);
			else
				previousResult.put(classIndices.get(record.getSecondRecord().getTableId()), 1.0);
		}else{
			previousResult = new HashMap<String, Double>();
			previousResult.put(classIndices.get(record.getSecondRecord().getTableId()), 1.0);
		}
		
		return previousResult;
	}

	@Override
	public HashMap<String, Double> initialise(Integer keyValue) {
		// TODO Auto-generated method stub
		return null;
	}

}
