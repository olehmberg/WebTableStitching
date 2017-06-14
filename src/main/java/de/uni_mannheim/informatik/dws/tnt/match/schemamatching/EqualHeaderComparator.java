package de.uni_mannheim.informatik.dws.tnt.match.schemamatching;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

public class EqualHeaderComparator implements Comparator<MatchableTableColumn, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;

	@Override
	public double compare(MatchableTableColumn record1, MatchableTableColumn record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {

		String h1 = record1.getHeader();
		String h2 = record2.getHeader();
		
		if("null".equals(h1)) {
			h1 = null;
		}
		if("null".equals(h2)) {
			h2 = null;
		}
		
		return Q.equals(h1, h2, false) ? 1.0 : 0.0;
	}

}
