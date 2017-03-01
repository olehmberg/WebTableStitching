/**
 * 
 */
package de.uni_mannheim.informatik.dws.t2k.match.blocking;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.index.IIndex;
import de.uni_mannheim.informatik.dws.t2k.index.io.InMemoryIndex;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class ClassAndTypeBasedSchemaBlockerTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.blocking.ClassAndTypeBasedSchemaBlocker#runBlocking(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)}.
	 * @throws FileNotFoundException 
	 */
	
	public void testRunBlocking() throws FileNotFoundException{
//		load web tables
		WebTables wb = new WebTables();
		wb = WebTables.loadWebTables(new File("src\\test\\resources\\webtables\\webtable1.csv"), false, true, false);
		
//		load kb table
		KnowledgeBase kb = new KnowledgeBase();
		IIndex index = new InMemoryIndex();
		kb = KnowledgeBase.loadKnowledgeBase(new File("src\\test\\resources\\kbtables\\"), index, false, new SurfaceForms(null, null));
		
//		create schema correspondences
		ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> schemaCorrespondences = new ResultSet<>();
		for(MatchableTableRow webCor : wb.getRecords().get()){
			for(MatchableTableRow kbCor : kb.getRecords().get()){
				String wLabel = webCor.getValues()[0].toString().toLowerCase();
				String kLabel = kbCor.getValues()[1].toString().toLowerCase();
				if(wLabel.equals(kLabel)){
					schemaCorrespondences.add(new Correspondence<MatchableTableRow, MatchableTableColumn>(webCor, kbCor, 1.0, null));
				}
			}
		}
		
		System.out.println(schemaCorrespondences.size());
		
//		create refinedClasses
		Map<Integer, Set<String>> refinedClasses = new HashMap<Integer, Set<String>>();
		Set<String> classes = new HashSet<String>();
		classes.add("kbtable1");
		refinedClasses.put(0, classes);
		
//		create blocker
		ClassAndTypeBasedSchemaBlocker blocker = new ClassAndTypeBasedSchemaBlocker(kb, refinedClasses);
		
//		check for null pointers
		assertNotNull(blocker.getKb());
		assertNotNull(blocker.getRefinedClasses());
		
		ResultSet<BlockedMatchable<MatchableTableColumn, MatchableTableRow>> blockedPairs = blocker.runBlocking(wb.getSchema(), kb.getSchema(), schemaCorrespondences, new DataProcessingEngine());

//		check for null pointer
		assertNotNull(blockedPairs);
		
//		check for size
		assertEquals(4, blockedPairs.size());
		
//		check for values
		for(BlockedMatchable<MatchableTableColumn, MatchableTableRow> pair : blockedPairs.get()){
			System.out.println(pair.getFirstRecord().getIdentifier());
			System.out.println(pair.getSecondRecord().getIdentifier());
		}
	}
}
