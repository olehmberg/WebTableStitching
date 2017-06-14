package de.uni_mannheim.informatik.dws.tnt.match.data;

import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.SchemaValueGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

public class MatchableTableColumnValueGenerator extends SchemaValueGenerator<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;

	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableValue, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableColumn>> resultCollector) {

		for(MatchableTableColumn c : record.getSchema()) {
			
			if(record.hasValue(c)) {
				
				Object value = record.get(c.getColumnIndex());
				
				if(value!=null) {
					resultCollector.next(new Pair<String, MatchableTableColumn>(value.toString(), c));
				}
				
			}
			
		}
		
	}	
		
}
