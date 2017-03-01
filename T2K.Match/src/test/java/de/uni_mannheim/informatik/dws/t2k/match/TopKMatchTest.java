package de.uni_mannheim.informatik.dws.t2k.match;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */

public class TopKMatchTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.TopKMatch#getTopKMatch(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine, int, double)}.
	 */
	
	public void testGetTopKMatch(){
//		create web table rows
		MatchableTableRow wr1 = new MatchableTableRow("wa", new String[] { "republican", "Donald Trump" }, 0, new DataType[] { DataType.string, DataType.string });
		MatchableTableRow wr2 = new MatchableTableRow("wb", new String[] { "democratic", "Hillary Clinton" }, 0, new DataType[] { DataType.string, DataType.string });
		
//		create kb table rows
		MatchableTableRow kr1 = new MatchableTableRow("ka", new Object[] { "http://dbpedia.org/resource/Republic", "republican", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		new MatchableTableRow("kb", new Object[] { "http://dbpedia.org/resource/CDU_Party", "cdu party", DateTime.parse("1940") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr3 = new MatchableTableRow("kc", new Object[] { "http://dbpedia.org/resource/Republic_Party_USA", "Republic Party USA", DateTime.parse("1920") }, 1, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr4 = new MatchableTableRow("kd", new Object[] { "http://dbpedia.org/resource/Democratic", "Democraticcss", DateTime.parse("1922") }, 1, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr5 = new MatchableTableRow("ke", new Object[] { "http://dbpedia.org/resource/Democratic_Party", "democratic party", DateTime.parse("1922") }, 2, new DataType[] { DataType.string, DataType.string, DataType.date });
		new MatchableTableRow("kf", new Object[] { "http://dbpedia.org/resource/CDU_Party", "cdu party", DateTime.parse("1940") }, 2, new DataType[] { DataType.string, DataType.string, DataType.date });
		new MatchableTableRow("kg", new Object[] { "http://dbpedia.org/resource/Republic_Party_USA", "Republic Party USA", DateTime.parse("1920") }, 3, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr8 = new MatchableTableRow("kh", new Object[] { "http://dbpedia.org/resource/Democratic_Party_India", "Democratic Party India", DateTime.parse("1922") }, 3, new DataType[] { DataType.string, DataType.string, DataType.date });
		
//		create table correspondences
		Correspondence<MatchableTableRow, MatchableTableColumn> cor1 = new Correspondence<MatchableTableRow, MatchableTableColumn>(wr1, kr1, 1.0, null);
		Correspondence<MatchableTableRow, MatchableTableColumn> cor2 = new Correspondence<MatchableTableRow, MatchableTableColumn>(wr1, kr3, 0.5, null);
		Correspondence<MatchableTableRow, MatchableTableColumn> cor3 = new Correspondence<MatchableTableRow, MatchableTableColumn>(wr2, kr4, 0.7, null);
		Correspondence<MatchableTableRow, MatchableTableColumn> cor4 = new Correspondence<MatchableTableRow, MatchableTableColumn>(wr2, kr5, 0.5, null);
		Correspondence<MatchableTableRow, MatchableTableColumn> cor5 = new Correspondence<MatchableTableRow, MatchableTableColumn>(wr2, kr8, 0.3, null);
		ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = new ResultSet<>();
		correspondences.add(cor1);
		correspondences.add(cor2);
		correspondences.add(cor3);
		correspondences.add(cor4);
		correspondences.add(cor5);
		
		ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> topkresult = TopKMatch.getTopKMatch(correspondences, new DataProcessingEngine(), 1, 0.5);
		
//		check for null pointer
		assertNotNull(topkresult);
		
//		check for size
		assertEquals(2, topkresult.size());
		
//		check for values
		for(Correspondence<MatchableTableRow, MatchableTableColumn> topcor : topkresult.get()){
		if(topcor.getFirstRecord().getIdentifier().equals("wa"))
			assertEquals(1.0, topcor.getSimilarityScore());
		else
			assertEquals(0.7, topcor.getSimilarityScore());
		}
	}
}
