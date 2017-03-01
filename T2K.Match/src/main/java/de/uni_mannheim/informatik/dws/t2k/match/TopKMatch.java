package de.uni_mannheim.informatik.dws.t2k.match;

import java.io.Serializable;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.TopKMatchGroupToRecordMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.TopKMatchRecordKeyMapper;
import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.Group;

public class TopKMatch implements Serializable{
	
	/**
	 * @author Sanikumar
	 */
	private static final long serialVersionUID = 1L;
	public TopKMatch() {
		
	}
		
	/**
	 * Gives top 'k' correspondences based on provided similarity score.
	 * 
	 * @param correspondences
	 * 			the correspondences (must not be null)
	 * @param proc
	 * 			the data processing engine (must not be null)
	 * @param k
	 * 			the number of correspondences to return (must not be null)
	 * @param similarityThreshold
	 * 			the similarity threshold (must not be null)
	 * @return the top 'k' correspondences
	 */
	
	public static <RecordType> ResultSet<RecordType> getTopKMatch(BasicCollection<RecordType> correspondences, DataProcessingEngine proc, final int k, final double similarityThreshold) {
		
//		group the correspondences based on first records' identifier and then select top record from each of the group 
		
		TopKMatchRecordKeyMapper<RecordType> t2kMatchRecordKeyMapper = new TopKMatchRecordKeyMapper<RecordType>();
		
		ResultSet<Group<String, RecordType>> candidates = proc.groupRecords(correspondences, t2kMatchRecordKeyMapper);
		
		TopKMatchGroupToRecordMapper<RecordType> t2kMatchGroupToRecordMapper = new TopKMatchGroupToRecordMapper<RecordType>(similarityThreshold, k);
		
		ResultSet<RecordType> onetooneMapping = proc.transform(candidates, t2kMatchGroupToRecordMapper);
		
    	
		return onetooneMapping;
	}
}
