/**
 * 
 */
package de.uni_mannheim.informatik.dws.t2k.match.rules;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.wdi.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;

/**
 * @author Sanikumar
 *
 */
public class DataTypeDependentSchemaMatchingRuleTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.rules.DataTypeDependentSchemaMatchingRule#apply(de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn, de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn, de.uni_mannheim.informatik.wdi.model.Correspondence)}
	 */
	public void testApply(){
//		create the table rows for web table 
		MatchableTableRow wr1 = new MatchableTableRow("wa", new Object[] { "republican", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.date });
		
//		create the table rows for knowledge base
		MatchableTableRow kr1 = new MatchableTableRow("ka", new Object[] { "http://dbpedia.org/resource/Republic", "republican", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr2 = new MatchableTableRow("kb", new Object[] { "http://dbpedia.org/resource/Republic_Party_(United States)", "Republican Party (United States)", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		
//		create the  table column for web table
		MatchableTableColumn wc1 = new MatchableTableColumn(0, 0, "partyName", DataType.string);
		MatchableTableColumn wc2 = new MatchableTableColumn(0, 1, "year", DataType.date);	
		
//		create the table column knowledge base
		@SuppressWarnings("unused")
		MatchableTableColumn kc1 = new MatchableTableColumn(0, 0, "URI", DataType.link);
		MatchableTableColumn kc2 = new MatchableTableColumn(0, 1, "rdf-schema#label", DataType.string);
		MatchableTableColumn kc3 = new MatchableTableColumn(0, 2, "yearFounded", DataType.date);	
		
//		create the dbpedia properties id
		Map<Integer, Map<Integer, Integer>> m = new HashMap<>();
		Map<Integer, Integer> map = new HashMap<>();
		map.put(0, 0);
		map.put(1, 1);
		map.put(2, 2);
		m.put(0, map);
		
//		create the schema correspondence
		Correspondence<MatchableTableRow, MatchableTableColumn> cor1 = new Correspondence<MatchableTableRow, MatchableTableColumn>(wr1, kr1, 1.0, null);
		Correspondence<MatchableTableRow, MatchableTableColumn> cor2 = new Correspondence<MatchableTableRow, MatchableTableColumn>(wr1, kr2, 0.25, null);
		
//		create the rule
		DataTypeDependentSchemaMatchingRule rule = new DataTypeDependentSchemaMatchingRule(0.25);
		rule.setComparatorForType(DataType.string, new MatchableTableRowComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), m, 0.5, new SurfaceForms(null, null), true));
		rule.setComparatorForType(DataType.date, new MatchableTableRowComparator<>(new WeightedDateSimilarity(1, 3, 5), m, 0.4));	
		rule.setRdfsLabelId(1);
		rule.setNumCorrespondences(2);
		rule.setNumVotesPerValue(0);
		
//		check for the parameters
		assertNotNull(rule.getComparators());
		assertNotNull(rule.getFinalThreshold());
		assertNotNull(rule.getNumCorrespondences());
		assertNotNull(rule.getNumVotesPerValue());
		assertNotNull(rule.getRdfsLabelId());
		
//		check for the similarity value
		assertNotNull(rule.apply(wc2, kc3, cor1));
		assertNull(rule.apply(wc1, kc2, cor1));
		assertNull(rule.apply(wc2, kc2, cor2));
		assertNotNull(rule.apply(wc2, kc3, cor2));
	}
	
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.rules.DataTypeDependentSchemaMatchingRule#aggregate(de.uni_mannheim.informatik.wdi.model.ResultSet, int, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)}
	 */
	public void testAggregate(){
		
	}
}
