package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;

public class TopKMatchRecordKeyMapper<RecordType> implements RecordKeyValueMapper<String, RecordType, RecordType>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public void mapRecord(RecordType record,
			DatasetIterator<Pair<String, RecordType>> resultCollector) {
		resultCollector.next(new Pair<String, RecordType>(((Correspondence<Matchable, Matchable>) record).getFirstRecord().getIdentifier(), record));
		
	}

}
