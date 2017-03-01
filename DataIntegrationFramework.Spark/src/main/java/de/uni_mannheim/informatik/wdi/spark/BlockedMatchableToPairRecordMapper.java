package de.uni_mannheim.informatik.wdi.spark;

import de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRuleWithVoting;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

public class BlockedMatchableToPairRecordMapper<SchemaElementType extends Matchable, RecordType> implements RecordMapper<BlockedMatchable<SchemaElementType, RecordType>, Pair<Correspondence<RecordType, SchemaElementType>, Correspondence<SchemaElementType, RecordType>>>{
	
	private SchemaMatchingRuleWithVoting<RecordType, SchemaElementType, SchemaElementType> rule;
	
	
	public BlockedMatchableToPairRecordMapper(
			SchemaMatchingRuleWithVoting<RecordType, SchemaElementType, SchemaElementType> rule) {
		this.rule = rule;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void mapRecord(
			BlockedMatchable<SchemaElementType, RecordType> record,
			DatasetIterator<Pair<Correspondence<RecordType, SchemaElementType>, Correspondence<SchemaElementType, RecordType>>> resultCollector) {
		for (Correspondence<RecordType, SchemaElementType> correspondence : record.getSchemaCorrespondences().get()) {
			// apply the matching rule
			Correspondence<SchemaElementType, RecordType> c = rule.apply(record.getFirstRecord(), record.getSecondRecord(), correspondence);
			
			if (c != null) {
				Pair<Correspondence<RecordType, SchemaElementType>,Correspondence<SchemaElementType, RecordType>> cor = new Pair<>(correspondence, c);
				
				// add the correspondences to the result
				resultCollector.next(cor);
			}

		}
	
	}

}
