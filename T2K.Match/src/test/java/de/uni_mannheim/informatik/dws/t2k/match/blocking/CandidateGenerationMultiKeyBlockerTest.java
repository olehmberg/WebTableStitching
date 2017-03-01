package de.uni_mannheim.informatik.dws.t2k.match.blocking;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.index.IIndex;
import de.uni_mannheim.informatik.dws.t2k.index.dbpedia.DBpediaIndexer;
import de.uni_mannheim.informatik.dws.t2k.index.io.InMemoryIndex;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import junit.framework.TestCase;

/**
 * 
 * @author Sanikumar
 *
 */
public class CandidateGenerationMultiKeyBlockerTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.blocking.CandidateGenerationMultiKeyBlocker#runBlocking(de.uni_mannheim.informatik.wdi.model.DataSet, boolean, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)}.
	 */
	public void testrunBlocking(){
		DataSet<MatchableTableRow, MatchableTableColumn> dataset1 = new DefaultDataSet<>();
		DataSet<MatchableTableRow, MatchableTableColumn> dataset2 = new DefaultDataSet<>();
		
//		create web table rows
		MatchableTableRow wr1 = new MatchableTableRow("wa", new String[] { "republican", "Donald Trump" }, 0, new DataType[] { DataType.string, DataType.string });
		MatchableTableRow wr2 = new MatchableTableRow("wb", new String[] { "democratic", "Hillary Clinton" }, 0, new DataType[] { DataType.string, DataType.string });
		dataset1.add(wr1);
		dataset1.add(wr2);
		
//		create kb table rows
		MatchableTableRow kr1 = new MatchableTableRow("ka", new Object[] { "http://dbpedia.org/resource/Republic", "republican", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr2 = new MatchableTableRow("kb", new Object[] { "http://dbpedia.org/resource/CDU_Party", "cdu party", DateTime.parse("1940") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		dataset2.add(kr1);
		dataset2.add(kr2);
		
//		create web table column
		MatchableTableColumn wc1 = new MatchableTableColumn(0, 0, "label", DataType.string);
		new MatchableTableColumn(0, 1, "candidate", DataType.string);

		new MatchableTableColumn(0, 0, "URI", DataType.link);
		MatchableTableColumn kc2 = new MatchableTableColumn(0, 1, "rdf-schema#label", DataType.string);
		new MatchableTableColumn(0, 2, "yearFounded", DataType.date);	

		
//		create schema correspondences between the web table and kb table
		Correspondence<MatchableTableColumn, MatchableTableRow> cor1 = new Correspondence<MatchableTableColumn, MatchableTableRow>(wc1, kc2, 0.5, null);
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = new ResultSet<>();
		schemaCorrespondences.add(cor1);
		
//		create data processing engine
		DataProcessingEngine engine = new DataProcessingEngine();
		
//		create class indices that maps table id to its name
		Map<Integer, String> classIndices = new HashMap<>();
		classIndices.put(0, "kbTable1");
		
//		create in memory index
		IIndex index = new InMemoryIndex();
		DBpediaIndexer indexer = new DBpediaIndexer();
		indexer.indexInstances(index, dataset2.getRecords(), classIndices, new SurfaceForms(null, null));
		
//		create blocker
		CandidateGenerationMultiKeyBlocker blocker = new CandidateGenerationMultiKeyBlocker(index);
		
//		check for null pointers
		assertNotNull(blocker.getIndex());
		assertNotNull(blocker.getMaxEditDistance());
		assertNotNull(blocker.getNumCandidates());
		
		ResultSet<BlockedMatchable<MatchableTableRow, MatchableTableColumn>> blockedPairs = blocker.runBlocking(dataset1, dataset2, schemaCorrespondences, engine);
		
//		check for null pointer
		assertNotNull(blockedPairs);
		
//		check for size, as only the block pair between 'wa' and 'ka' will be created
		assertEquals(1, blockedPairs.size());
		
//		check for values
		for(BlockedMatchable<MatchableTableRow, MatchableTableColumn> pair : blockedPairs.get()){
			assertEquals("wa", pair.getFirstRecord().getIdentifier());
			assertEquals("ka", pair.getSecondRecord().getIdentifier());
		}
	}
}
