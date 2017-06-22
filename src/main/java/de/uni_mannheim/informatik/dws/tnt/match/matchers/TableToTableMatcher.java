package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.DeterminantBasedDuplicateDetectionRule;
import de.uni_mannheim.informatik.dws.tnt.match.recordmatching.DeterminantRecordBlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.CorrespondenceTransitivityMaterialiser;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.EqualHeaderComparator;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.ValueBasedSchemaMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased.DuplicateBasedSchemaVotingRule;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.DisjointHeaderMatchingRule;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.refinement.GraphBasedRefinement;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.VotingAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.NoSchemaBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.StandardRecordBlocker;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;

public abstract class TableToTableMatcher extends TnTTask {

	public String getTaskName() {
		return this.getClass().getSimpleName();
	}
	
	private long runtime = 0;
	/**
	 * @return the runtime
	 */
	public long getRuntime() {
		return runtime;
	}
	
	private boolean verbose = false;
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	protected WebTables web;
	protected Map<String, Set<String>> disjointHeaders;
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences;
	
	/**
	 * @param disjointHeaders the disjointHeaders to set
	 */
	public void setDisjointHeaders(Map<String, Set<String>> disjointHeaders) {
		this.disjointHeaders = disjointHeaders;
	}
	/**
	 * @return the disjointHeaders
	 */
	public Map<String, Set<String>> getDisjointHeaders() {
		return disjointHeaders;
	}
	
	/**
	 * @param web the web to set
	 */
	public void setWebTables(WebTables web) {
		this.web = web;
	}

	public Processable<Correspondence<MatchableTableColumn, Matchable>> getSchemaCorrespondences() {
		return schemaCorrespondences;
	}
	
	@Override
	public void initialise() throws Exception {

	}
	@Override
	public void match() throws Exception {
		long start = System.currentTimeMillis();
		
		runMatching();
		
		schemaCorrespondences = runPairwiseRefinement(web, getSchemaCorrespondences(), new DisjointHeaders(disjointHeaders));
		
		schemaCorrespondences = runGraphbasedRefinement(getSchemaCorrespondences(), new DisjointHeaders(disjointHeaders));
		
		schemaCorrespondences = applyTransitivity(schemaCorrespondences);
		
    	long end = System.currentTimeMillis();
    	
    	runtime = end-start;
    	
    	System.out.println(String.format("[%s] Matching finished after %s", this.getClass().getName(), DurationFormatUtils.formatDurationHMS(runtime)));
	}
	
	protected abstract void runMatching();
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> runValueBased(WebTables web) {
		ValueBasedSchemaMatcher matcher = new ValueBasedSchemaMatcher();
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> cors = matcher.run(web.getRecords());
		
//		for(Correspondence<MatchableTableColumn, MatchableValue> cor : cors.get()) {
//			MatchableTableColumn c1 = cor.getFirstRecord();
//			MatchableTableColumn c2 = cor.getSecondRecord();
//			System.out.println(String.format("{%d}[%d]%s <-> {%d}[%d]%s (%f)", c1.getTableId(), c1.getColumnIndex(), c1.getHeader(), c2.getTableId(), c2.getColumnIndex(), c2.getHeader(), cor.getSimilarityScore()));
//			
//			for(Correspondence<MatchableValue, Matchable> cause : cor.getCausalCorrespondences().get()) {
//				System.out.println(String.format("\t%s (%f)", cause.getFirstRecord().getValue(), cause.getSimilarityScore()));
//			}
//		}
		
		return cors;
	}
	
	protected Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> findDuplicates(
			WebTables web,
			Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> detCors) {
		
		MatchingEngine<MatchableTableRow, MatchableTableDeterminant> engine = new MatchingEngine<>();
		
		DataSet<MatchableTableRow, MatchableTableDeterminant> ds = new ParallelHashedDataSet<>(web.getRecords().get());
		DeterminantRecordBlockingKeyGenerator bkg = new DeterminantRecordBlockingKeyGenerator();
		DeterminantBasedDuplicateDetectionRule rule = new DeterminantBasedDuplicateDetectionRule(0.0);
		
		Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> duplicates = engine.runDuplicateDetection(ds, detCors, rule, new StandardRecordBlocker<>(bkg));
		
//		for(Correspondence<MatchableTableRow, MatchableTableDeterminant> cor : duplicates.get()) {
//			System.out.println(String.format("%s\n%s", cor.getFirstRecord().format(20), cor.getSecondRecord().format(20)));
//			for(Correspondence<MatchableTableDeterminant, Matchable> c : cor.getCausalCorrespondences().get()) {
//				System.out.println(String.format("\t[%d]{%s}<=>[%d]{%s}", 
//						c.getFirstRecord().getTableId(),
//						StringUtils.join(Q.project(c.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderWithIndexProjection()), ","),
//						c.getSecondRecord().getTableId(),
//						StringUtils.join(Q.project(c.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderWithIndexProjection()), ",")
//						));
//			}
//		}
		
		return duplicates;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> runDuplicateBasedSchemaMatching(WebTables web, Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> duplicates) {
		
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
		DuplicateBasedSchemaVotingRule rule = new DuplicateBasedSchemaVotingRule(0.0);
		VotingAggregator<MatchableTableColumn, MatchableTableRow> voteAggregator = new VotingAggregator<>(false, 1.0);
		NoSchemaBlocker<MatchableTableColumn, MatchableTableRow> schemaBlocker = new NoSchemaBlocker<>();
		
		// we must flatten the duplicates, such that each determinant that created a duplicate is processed individually by the matching rule
		// only then can we group the votes by determinant and make sense of the number of votes
		Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> flatDuplicates = new ProcessableCollection<>();
		Correspondence.flatten(duplicates, flatDuplicates);
		
//		for(Correspondence<MatchableTableRow, MatchableTableDeterminant> c : duplicates.sort((c)-> c.getIdentifiers()).get()) {
//			System.out.println(c.getIdentifiers());
//			for(Correspondence<MatchableTableDeterminant, Matchable> cause : c.getCausalCorrespondences().sort((c1)-> c1.getIdentifiers()).get()) {
//				System.out.println("\t" + cause.getIdentifiers());
//			}
//		}
		
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = engine.runDuplicateBasedSchemaMatching(web.getSchema(), web.getSchema(), flatDuplicates, rule, null, voteAggregator, schemaBlocker);
		
//		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
//			
//			System.out.println(String.format("{%d}%s<->{%d}%s (%f)", cor.getFirstRecord().getTableId(), cor.getFirstRecord().getHeader(), cor.getSecondRecord().getTableId(), cor.getSecondRecord().getHeader(), cor.getSimilarityScore()));
//			
//			for(Correspondence<MatchableTableRow, Matchable> cause : cor.getCausalCorrespondences().sort((c)->c.getIdentifiers()).get()) {
//				
//				System.out.println(String.format("\t{%d}%s<->{%d}%s (%f)", 
//						cause.getFirstRecord().getTableId(),
//						cause.getFirstRecord().getIdentifier(),
//						cause.getSecondRecord().getTableId(),
//						cause.getSecondRecord().getIdentifier(),
//						cause.getSimilarityScore()));
//				
//				for(Correspondence<Matchable, Matchable> cc : cause.getCausalCorrespondences().get()) {
//					
//					System.out.println(String.format("\t\t{%s}<->{%s} (%f)", 
//							cc.getFirstRecord().getIdentifier(),
//							cc.getSecondRecord().getIdentifier(),
//							cc.getSimilarityScore()));
//					
//				}
//				
//			}
//		}
		
		// filter out correspondences with similarity < 1.0
		// these are schema correspondences that do not hold for all duplicates that were created for a certain determinant
		// (either the correspondence is wrong, or the attributes are not in the closure of the determinant)
		// if there is another determinant for which this correspondence hold, we might still have it after filtering
		
//		schemaCorrespondences = schemaCorrespondences.filter((c)->c.getSimilarityScore()>=1.0);
		
		//TODO group remaining correspondences to merge all determinants that created them
		// i.e., each schema correspondence should only exist once with all the determinant correspondences that created it as causal correspondences
		
		return schemaCorrespondences;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> runPairwiseRefinement(WebTables web, Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences, DisjointHeaders disjointHeaders) throws Exception {
		MatchingEngine<MatchableTableRow, MatchableTableColumn> engine = new MatchingEngine<>();
		
		// run label-based matcher with equality comparator to generate correspondences
		EqualHeaderComparator labelComparator = new EqualHeaderComparator();
		
		Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> labelBasedCors = engine.runLabelBasedSchemaMatching(web.getSchema(), labelComparator, 1.0);
		
//		for(Correspondence<MatchableTableColumn, MatchableTableColumn> cor : labelBasedCors.get()) {
//			System.out.println(String.format("[LabelBased] %s <-> %s", cor.getFirstRecord(), cor.getSecondRecord()));
//		}
		
		// combine label- and value-based correspondences
		Processable<Correspondence<MatchableTableColumn, Matchable>> cors = schemaCorrespondences.append(Correspondence.toMatchable(labelBasedCors)).distinct();
		
		// apply disjoint header rule to remove inconsistencies
		DisjointHeaderMatchingRule rule = new DisjointHeaderMatchingRule(disjointHeaders, 0.0);
		rule.setVerbose(verbose);

		cors = cors.map(rule);
		
		return cors;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> runGraphbasedRefinement(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences, DisjointHeaders disjointHeaders) {
		
		//TODO create new matching components:
		// - graph-based blocker: receives all correspondences, partitions them and creates the partitions as blocks
		// - graph-based matching rule: receives a partition, applies its rule to the partition, creates the altered edges as correspondences
		
		
		GraphBasedRefinement refiner = new GraphBasedRefinement(true); 
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> refined = refiner.match(correspondences, disjointHeaders);
		
		return refined;
	}
	
	protected Processable<Correspondence<MatchableTableColumn, Matchable>> applyTransitivity(Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences) {
		CorrespondenceTransitivityMaterialiser transitivity = new CorrespondenceTransitivityMaterialiser();
		Processable<Correspondence<MatchableTableColumn, Matchable>> transitiveCorrespondences = transitivity.aggregate(correspondences);
    	correspondences = correspondences.append(transitiveCorrespondences);
    	correspondences = correspondences.distinct();
    	
    	return correspondences;
	}
}
