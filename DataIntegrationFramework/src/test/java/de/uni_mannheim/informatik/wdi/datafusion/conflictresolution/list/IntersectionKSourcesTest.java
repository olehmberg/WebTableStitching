package de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.list;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FusableValue;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import junit.framework.TestCase;

public class IntersectionKSourcesTest extends TestCase {

	public void testResolveConflictCollectionOfFusableValueOfListOfValueTypeRecordType() {
		IntersectionKSources<String, Movie, DefaultSchemaElement> crf = new IntersectionKSources<>(
				2);
		List<FusableValue<List<String>, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(), null, null));
		FusedValue<List<String>, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(0, resolvedValue.getValue().size());
	}

	public void testResolveConflictCollectionOfFusableValueOfListOfValueTypeRecordType2() {
		IntersectionKSources<String, Movie, DefaultSchemaElement> crf = new IntersectionKSources<>(
				2);
		List<FusableValue<List<String>, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h0", "h1"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h1", "h2"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h2", "h3"), null, null));
		FusedValue<List<String>, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(2, resolvedValue.getValue().size());
	}
	
	public void testResolveConflictCollectionOfFusableValueOfListOfValueTypeRecordType3() {
		IntersectionKSources<String, Movie, DefaultSchemaElement> crf = new IntersectionKSources<>(
				1);
		List<FusableValue<List<String>, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h0", "h1"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h1", "h2"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h2", "h3"), null, null));
		FusedValue<List<String>, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(4, resolvedValue.getValue().size());
	}
	
	public void testResolveConflictCollectionOfFusableValueOfListOfValueTypeRecordType4() {
		IntersectionKSources<String, Movie, DefaultSchemaElement> crf = new IntersectionKSources<>(
				3);
		List<FusableValue<List<String>, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h0", "h1"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h1", "h2"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h2", "h1"), null, null));
		FusedValue<List<String>, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(1, resolvedValue.getValue().size());
	}
}
