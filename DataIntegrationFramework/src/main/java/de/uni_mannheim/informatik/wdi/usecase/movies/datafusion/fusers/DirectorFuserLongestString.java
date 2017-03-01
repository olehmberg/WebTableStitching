/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.wdi.usecase.movies.datafusion.fusers;

import de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.string.LongestString;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.model.RecordGroup;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

/**
 * {@link AttributeValueFuser} for the director of {@link Movie}s.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class DirectorFuserLongestString extends
		AttributeValueFuser<String, Movie, DefaultSchemaElement> {

	public DirectorFuserLongestString() {
		super(new LongestString<Movie, DefaultSchemaElement>());
	}

	@Override
	public boolean hasValue(Movie record, Correspondence<DefaultSchemaElement, Movie> correspondence) {
		return record.hasValue(Movie.DIRECTOR);
	}

	@Override
	protected String getValue(Movie record, Correspondence<DefaultSchemaElement, Movie> correspondence) {
		return record.getDirector();
	}

	@Override
	public void fuse(RecordGroup<Movie, DefaultSchemaElement> group, Movie fusedRecord, ResultSet<Correspondence<DefaultSchemaElement, Movie>> schemaCorrespondences, DefaultSchemaElement schemaElement) {
		FusedValue<String, Movie, DefaultSchemaElement> fused = getFusedValue(group, schemaCorrespondences, schemaElement);
		fusedRecord.setDirector(fused.getValue());
		fusedRecord.setAttributeProvenance(Movie.DIRECTOR,
				fused.getOriginalIds());
	}

}
