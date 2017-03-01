package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;

public class ClassDistributionRecordKeyMapper implements RecordKeyValueMapper<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void mapRecord(
			Correspondence<MatchableTableRow, MatchableTableColumn> record,
			DatasetIterator<Pair<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
		resultCollector.next(new Pair<Integer, Correspondence<MatchableTableRow,MatchableTableColumn>>(record.getFirstRecord().getTableId(), record));
	}

}
