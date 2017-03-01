package de.uni_mannheim.informatik.wdi.matching.blocking;

import java.io.Serializable;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * Contract for the result of every blocking operation (even no-blocking). Gives access to the records that should be matched and the schema correspondences between them.
 * Default implementation is {@link MatchingTask}
 * @author Oliver
 *
 * @param <RecordType>
 * @param <SchemaElementType>
 */
public interface BlockedMatchable<RecordType extends Matchable, SchemaElementType> extends Serializable{

	RecordType getFirstRecord();
	RecordType getSecondRecord();
	ResultSet<Correspondence<SchemaElementType, RecordType>> getSchemaCorrespondences();
	
}
