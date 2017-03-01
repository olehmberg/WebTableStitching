package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.wdi.matching.MatchingTask;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

public class JoinedCorrespondenceToMatchingTaskMapper<RecordType extends Matchable, SchemaElementType> implements RecordMapper<Pair<Iterable<Correspondence<RecordType, SchemaElementType>>, Iterable<Correspondence<SchemaElementType, RecordType>>>, BlockedMatchable<RecordType, SchemaElementType>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void mapRecord(
			Pair<Iterable<Correspondence<RecordType, SchemaElementType>>, Iterable<Correspondence<SchemaElementType, RecordType>>> record,
			DatasetIterator<BlockedMatchable<RecordType, SchemaElementType>> resultCollector) {
		ResultSet<Correspondence<SchemaElementType, RecordType>> result2 = new ResultSet<Correspondence<SchemaElementType, RecordType>>();
		for(Correspondence<SchemaElementType, RecordType> ir : record.getSecond())
			result2.add(ir);
		for(Correspondence<RecordType, SchemaElementType> ir : record.getFirst())
			resultCollector.next(new MatchingTask<RecordType, SchemaElementType>(ir.getFirstRecord(), ir.getSecondRecord(), result2));
		
	}

}
