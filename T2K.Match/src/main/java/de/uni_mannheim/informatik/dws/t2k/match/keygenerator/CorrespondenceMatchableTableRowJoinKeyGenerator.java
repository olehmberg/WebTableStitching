package de.uni_mannheim.informatik.dws.t2k.match.keygenerator;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.processing.Function;

public class CorrespondenceMatchableTableRowJoinKeyGenerator<RecordType, SchemaElementType> implements Function<Integer, Correspondence<RecordType, SchemaElementType>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Integer execute(Correspondence<RecordType, SchemaElementType> input) {
		return ((MatchableTableRow) input.getFirstRecord()).getTableId();
	}

}
