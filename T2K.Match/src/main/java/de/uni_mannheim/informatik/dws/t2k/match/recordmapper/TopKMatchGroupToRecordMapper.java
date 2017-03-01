package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import java.util.Comparator;
import java.util.List;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

public class TopKMatchGroupToRecordMapper<RecordType> implements RecordMapper<Group<String, RecordType>, RecordType>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private double similarityThreshold;
	private int k;

	public TopKMatchGroupToRecordMapper(double similarityThreshold, int k) {
		this.similarityThreshold = similarityThreshold;
		this.k = k;
	}


	@SuppressWarnings("unchecked")
	@Override
	public void mapRecord(Group<String, RecordType> record,
			DatasetIterator<RecordType> resultCollector) {
		
//		sort the records based on their similarity score (from high to low)
		List<RecordType> intermediateResult = Q.sort(record.getRecords().get(), new Comparator<RecordType>() {

			@Override
			public int compare(RecordType o1, RecordType o2) {
				return Double.compare(((Correspondence<Matchable, Matchable>) o2).getSimilarityScore(), ((Correspondence<Matchable, Matchable>) o1).getSimilarityScore());
			}
		});
		
		// add top k to final result
		int count=0;
		for(RecordType r : intermediateResult){
			if(count<k && ((Correspondence<Matchable, Matchable>) r).getSimilarityScore()>=similarityThreshold){
				resultCollector.next(r);
				count++;
			}else
				break;
				
		}
	}

}
