package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import java.util.HashMap;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

public class ClassDecisionCorrespondenceToCorrespondenceRecordMapper implements RecordMapper<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private HashMap<Integer, Double> classWeight;
	
	
	public ClassDecisionCorrespondenceToCorrespondenceRecordMapper(
			HashMap<Integer, Double> classWeight) {
		this.classWeight = classWeight;
	}


	@Override
	public void mapRecord(
			Correspondence<MatchableTableRow, MatchableTableColumn> record,
			DatasetIterator<Correspondence<MatchableTableRow, MatchableTableColumn>> resultCollector) {
		if(classWeight.containsKey(record.getSecondRecord().getTableId())){
			record.setsimilarityScore(record.getSimilarityScore() + classWeight.get(record.getSecondRecord().getTableId()));
		}
			resultCollector.next(record);	
	}

}
