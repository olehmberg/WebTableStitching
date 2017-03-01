package de.uni_mannheim.informatik.dws.t2k.match.keygenerator;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.processing.Function;

public class CorrespondenceMatchableTableColumnJoinKeyGenerator<RecordType, SchemaElementType> implements Function<Integer, Correspondence<SchemaElementType, RecordType>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Integer execute(Correspondence<SchemaElementType, RecordType> input) {
		return  ((MatchableTableColumn) input.getFirstRecord()).getTableId();
	}

}
