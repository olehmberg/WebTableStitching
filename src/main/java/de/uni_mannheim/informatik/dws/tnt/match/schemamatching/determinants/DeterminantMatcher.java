package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.determinants;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;

public class DeterminantMatcher {

	public 
	<T extends Matchable, CorT extends Correspondence<MatchableTableColumn, T>> 
	Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> 
	propagateDependencies(
		Processable<CorT> schemaCorrespondences,
		DataSet<MatchableTableDeterminant, MatchableTableColumn> determinants) {
		
    	Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> determinantCorrespondences = null;
    	
    	System.out.println(String.format("Key propagation, %d initial keys", determinants.size()));
    	
    	
    	// REFACTORED VERSION
    	// uses the standard blocker, but is much slower than the original version
//    	// match the determinants via the schema correspondences
//    	DeterminantBlockingKeyGenerator bkg = new DeterminantBlockingKeyGenerator();
//    	DeterminantMatchingRule mr = new DeterminantMatchingRule(0.0);
//    	MatchingEngine<MatchableTableDeterminant, MatchableTableColumn> engine = new MatchingEngine<>();    	
//    	// run a blocker with schema correspondences that propagates the keys
//    	determinantCorrespondences = engine.runDuplicateDetection(determinants, schemaCorrespondences, mr, new StandardBlocker<>(bkg));

    	// ORIGINAL VERSION
    	DeterminantBySchemaCorrespondenceBlocker detBlocker = new DeterminantBySchemaCorrespondenceBlocker(true);
    	determinantCorrespondences = detBlocker.runBlocking(determinants, Correspondence.toMatchable(schemaCorrespondences));
    	
    	// then consolidate the created determinants, i.e., create a dataset with the new determinants
    	CandidateKeyConsolidator<MatchableTableColumn> keyConsolidator = new CandidateKeyConsolidator<>(true);
    	determinants = new HashedDataSet<MatchableTableDeterminant, MatchableTableColumn>(keyConsolidator.run(new ProcessableCollection<MatchableTableDeterminant>(), determinantCorrespondences).get());

    	System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", 1, determinants.size(), determinantCorrespondences.size()));
    	
    	int last = determinants.size();
    	int round = 2;
    	do {

    		last = determinants.size();

    		// match the determinants again and propagate
    		determinantCorrespondences = detBlocker.runBlocking(determinants, Correspondence.toMatchable(schemaCorrespondences));
//    		determinantCorrespondences = engine.runDuplicateDetection(determinants, schemaCorrespondences, mr, new StandardBlocker<>(bkg));
    		
    		// consolidate results
    		determinants = new HashedDataSet<MatchableTableDeterminant, MatchableTableColumn>(keyConsolidator.run(new ProcessableCollection<MatchableTableDeterminant>(), determinantCorrespondences).get());

    		System.out.println(String.format("Key Propagation (Round %d): %d keys / %d key correspondences", round++, determinants.size(), determinantCorrespondences.size()));
    	} while(determinants.size()!=last);

    	// filter out determinants that only consist of context columns
    	Iterator<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> corIt = determinantCorrespondences.get().iterator();
    	while(corIt.hasNext()) {
    		Correspondence<MatchableTableDeterminant, MatchableTableColumn> cor = corIt.next();
    		if(Q.all(cor.getFirstRecord().getColumns(), new Func<Boolean, MatchableTableColumn>() {

				@Override
				public Boolean invoke(MatchableTableColumn in) {
					return ContextColumns.isContextColumn(in);
				}
			})) {
    			corIt.remove();
    		}
    	}
    	
    	return determinantCorrespondences;

	}

	public DataSet<MatchableTableDeterminant, MatchableTableColumn> createFDs(int minDeterminantSize, Collection<Table> tables, DataSet<MatchableTableColumn, MatchableTableColumn> schema) {
		
		// create a collection of FD left-hand sides
		DataSet<MatchableTableDeterminant, MatchableTableColumn> fds = new HashedDataSet<>();
		for(Table t : tables) {
			for(Collection<TableColumn> lhs : t.getSchema().getFunctionalDependencies().keySet()) {
				
				// make sure that the LHS is a determinant in the fd (and not {}->X)
				if(lhs.size()>0) {
				
					// do not consider trivial fds
					if(lhs.size()==1) {
						Collection<TableColumn> rhs = t.getSchema().getFunctionalDependencies().get(lhs);
						if(lhs.equals(rhs)) {
							continue;
						}
					} 
					
					if(lhs.size()<minDeterminantSize) {
						continue;
					}
					
					Set<MatchableTableColumn> columns = new HashSet<>();
					
					for(TableColumn c : lhs) {
						MatchableTableColumn col = schema.getRecord(c.getIdentifier());
						columns.add(col);
					}
					
					MatchableTableDeterminant k = new MatchableTableDeterminant(t.getTableId(), columns);
					fds.add(k);
				}
			}
		}
		
		return fds;
	}
	
	public DataSet<MatchableTableDeterminant, MatchableTableColumn> createCandidateKeys(int minDeterminantSize, Collection<Table> tables, DataSet<MatchableTableColumn, MatchableTableColumn> schema) {
		
		// create a collection of FD left-hand sides
		DataSet<MatchableTableDeterminant, MatchableTableColumn> fds = new HashedDataSet<>();
		for(Table t : tables) {
			for(Collection<TableColumn> lhs : t.getSchema().getCandidateKeys()) {
				
				// make sure that the LHS is a determinant in the fd (and not {}->X)
				if(lhs.size()>0) {
				
					// do not consider trivial fds
					if(lhs.size()==1) {
						Collection<TableColumn> rhs = t.getSchema().getFunctionalDependencies().get(lhs);
						if(lhs.equals(rhs)) {
							continue;
						}
					} 
					
					if(lhs.size()<minDeterminantSize) {
						continue;
					}
					
					Set<MatchableTableColumn> columns = new HashSet<>();
					
					for(TableColumn c : lhs) {
						MatchableTableColumn col = schema.getRecord(c.getIdentifier());
						columns.add(col);
					}
					
					MatchableTableDeterminant k = new MatchableTableDeterminant(t.getTableId(), columns);
					fds.add(k);
				}
			}
		}
		
		return fds;
	}
	
	public DataSet<MatchableTableDeterminant, MatchableTableColumn> createEntityLabels(Collection<Table> tables, DataSet<MatchableTableColumn, MatchableTableColumn> schema) {

		DataSet<MatchableTableDeterminant, MatchableTableColumn> fds = new HashedDataSet<>();
		for(Table t : tables) {
			
			if(t.getSubjectColumn()!=null) {
				
				Set<MatchableTableColumn> columns = new HashSet<>();
				columns.add(schema.getRecord(t.getSubjectColumn().getIdentifier()));
				
				MatchableTableDeterminant k = new MatchableTableDeterminant(t.getTableId(), columns);
				fds.add(k);
			}
		}
		
		return fds;
	}
}
