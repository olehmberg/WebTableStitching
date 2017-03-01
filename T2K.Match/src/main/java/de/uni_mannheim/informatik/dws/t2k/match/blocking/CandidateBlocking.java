package de.uni_mannheim.informatik.dws.t2k.match.blocking;

import java.io.Serializable;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.keygenerator.CorrespondenceMatchableTableColumnJoinKeyGenerator;
import de.uni_mannheim.informatik.dws.t2k.match.keygenerator.CorrespondenceMatchableTableRowJoinKeyGenerator;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.JoinedCorrespondenceToMatchingTaskMapper;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.matching.blocking.Blocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.Function;

public class CandidateBlocking extends Blocker<MatchableTableRow, MatchableTableColumn> implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences;
	public CandidateBlocking(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences) {
		this.correspondences = correspondences;
	}
	
	/**
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.Blocker#runBlocking(DataSet, DataSet, ResultSet, DataProcessingEngine)
	 */
	@Override
	public ResultSet<BlockedMatchable<MatchableTableRow, MatchableTableColumn>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset1,
			DataSet<MatchableTableRow, MatchableTableColumn> dataset2,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		// to get the matching schema correspondences, we have to join on both table ids
		// as we want a list of all schema correspondences, we use coGroup instead of join (which gives us collections for all objects with the same grouping key)
		
		Function<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>> groupingKeyGenerator1 = new CorrespondenceMatchableTableRowJoinKeyGenerator<>();
		
		Function<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>> groupingKeyGenerator2 = new CorrespondenceMatchableTableColumnJoinKeyGenerator<>();
		
		JoinedCorrespondenceToMatchingTaskMapper<MatchableTableRow, MatchableTableColumn> correspondenceToMatchingTaskMapper = new JoinedCorrespondenceToMatchingTaskMapper<>();
		
		ResultSet<BlockedMatchable<MatchableTableRow, MatchableTableColumn>> result = engine.coGroup(correspondences, schemaCorrespondences, groupingKeyGenerator1, groupingKeyGenerator2, correspondenceToMatchingTaskMapper);
		
		calculatePerformance(dataset1, dataset2, result);
		
		return result;
		
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.Blocker#runBlocking(de.uni_mannheim.informatik.wdi.model.DataSet, boolean, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)
	 */
	@Override
	public ResultSet<BlockedMatchable<MatchableTableRow, MatchableTableColumn>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset, boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences,
			DataProcessingEngine engine) {
		// TODO Auto-generated method stub
		return null;
	}


}
