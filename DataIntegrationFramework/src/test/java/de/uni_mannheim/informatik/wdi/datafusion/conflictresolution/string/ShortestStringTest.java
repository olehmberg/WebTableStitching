package de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.string;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FusableValue;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

public class ShortestStringTest extends TestCase {

	public void testResolveConflictCollectionOfFusableValueOfStringRecordType() {
		ShortestString<Movie, DefaultSchemaElement> crf = new ShortestString<>();
		List<FusableValue<String, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hello", null, null));
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hello", null, null));
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hello moto", null,
				null));
		FusedValue<String, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals("hello", resolvedValue.getValue());
	}

	public void testResolveConflictCollectionOfFusableValueOfStringRecordType2() {
		ShortestString<Movie, DefaultSchemaElement> crf = new ShortestString<>();
		List<FusableValue<String, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hello", null, null));
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("", null, null));
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hello moto", null,
				null));
		FusedValue<String, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals("", resolvedValue.getValue());
	}

	public void testResolveConflictCollectionOfFusableValueOfStringRecordType3() {
		ShortestString<Movie, DefaultSchemaElement> crf = new ShortestString<>();
		List<FusableValue<String, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		FusedValue<String, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(null, resolvedValue.getValue());
	}

}
