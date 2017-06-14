package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.matching.rules.MatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;

public class DisjointHeaderMatchingRule extends MatchingRule<MatchableTableColumn, Matchable> {

	private static final long serialVersionUID = 1L;


	public DisjointHeaderMatchingRule(DisjointHeaders disjointHeaders, double threshold) {
		super(threshold);
		this.disjointHeaders = disjointHeaders;
	}

	private DisjointHeaders disjointHeaders;

	private boolean verbose=false;
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	public boolean isVerbose() {
		return verbose;
	}

	@Override
	public void mapRecord(Correspondence<MatchableTableColumn, Matchable> record,
			DataIterator<Correspondence<MatchableTableColumn, Matchable>> resultCollector) {
		String h1 = record.getFirstRecord().getHeader();
		String h2 = record.getSecondRecord().getHeader();
		
		if(!disjointHeaders.getDisjointHeaders(h1).contains(h2)
				&& !disjointHeaders.getDisjointHeaders(h2).contains(h1)) {
			resultCollector.next(record);
		} else if(verbose) {
			System.out.println(String.format("[DisjointHeaderMatchingRule] Removing %s <-> %s", record.getFirstRecord(), record.getSecondRecord()));
		}
		
	}


	@Override
	public double compare(MatchableTableColumn record1, MatchableTableColumn record2,
			Correspondence<Matchable, Matchable> schemaCorrespondence) {
		return 0;
	}

}
