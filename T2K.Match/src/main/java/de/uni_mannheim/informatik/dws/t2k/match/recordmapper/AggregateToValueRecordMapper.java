package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

public class AggregateToValueRecordMapper<TKey, TValue> implements RecordMapper<Pair<TKey, TValue>, TValue>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void mapRecord(
			Pair<TKey, TValue> record,
			DatasetIterator<TValue> resultCollector) {
		resultCollector.next(record.getSecond());
	}

}
