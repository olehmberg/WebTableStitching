package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;

/**
 * 
 * Groups correspondences by the identifiers of their two records (directed)
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class GroupCorrespondencesRecordKeyMapper implements RecordKeyValueMapper<String, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>{

	private static final long serialVersionUID = 1L;

	@Override
	public void mapRecord(
			Correspondence<MatchableTableColumn, MatchableTableRow> record,
			DatasetIterator<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {
		resultCollector.next(new Pair<String, Correspondence<MatchableTableColumn,MatchableTableRow>>((record.getFirstRecord().getIdentifier() + record.getSecondRecord().getIdentifier()), record));
	}

}
