package de.uni_mannheim.informatik.wdi.matching;

import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * Default implementation of a BlockedMatchable
 * @author Oliver
 *
 * @param <RecordType>
 * @param <SchemaElementType>
 */
public class MatchingTask<RecordType extends Matchable, SchemaElementType> implements BlockedMatchable<RecordType, SchemaElementType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private RecordType first;
	private RecordType second;
	private ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences;
	
	@Override
	public RecordType getFirstRecord() {
		return first;
	}

	@Override
	public RecordType getSecondRecord() {
		return second;
	}

	@Override
	public ResultSet<Correspondence<SchemaElementType, RecordType>> getSchemaCorrespondences() {
		return schemaCorrespondences;
	}
	
	public MatchingTask() {
		
	}
	
	public MatchingTask(RecordType first, RecordType second, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		this.first = first;
		this.second = second;
		this.schemaCorrespondences = schemaCorrespondences;
	}
}
