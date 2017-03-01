package de.uni_mannheim.informatik.wdi.matching.blocking;

import de.uni_mannheim.informatik.wdi.matching.MatchingTask;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

public class RecordCorrespondenceBasedBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends DatasetLevelBlocker<RecordType,SchemaElementType> {

	protected ResultSet<Correspondence<RecordType, SchemaElementType>> correspondences;
	
	public RecordCorrespondenceBasedBlocker(ResultSet<Correspondence<RecordType, SchemaElementType>> correspondences) {
		this.correspondences = correspondences;
	}
	
	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {		
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ResultSet<>();
		
		for(Correspondence<RecordType, SchemaElementType> c : correspondences.get()) {
			result.add(new MatchingTask<RecordType, SchemaElementType>(dataset1.getRecord(c.getFirstRecord().getIdentifier()), dataset2.getRecord(c.getSecondRecord().getIdentifier()), schemaCorrespondences));
		}
		
		return result;
	}

	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			DataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ResultSet<>();
		
		for(Correspondence<RecordType, SchemaElementType> c : correspondences.get()) {
			result.add(new MatchingTask<RecordType, SchemaElementType>(dataset.getRecord(c.getFirstRecord().getIdentifier()), dataset.getRecord(c.getSecondRecord().getIdentifier()), schemaCorrespondences));
		}
		
		return result;
	}
	
}
