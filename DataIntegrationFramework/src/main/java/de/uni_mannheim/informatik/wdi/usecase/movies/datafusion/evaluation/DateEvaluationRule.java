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
package de.uni_mannheim.informatik.wdi.usecase.movies.datafusion.evaluation;

import de.uni_mannheim.informatik.wdi.datafusion.EvaluationRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

/**
 * {@link EvaluationRule} for the date of {@link Movie}s. The rule simply
 * compares the year of the dates of two {@link Movie}s.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class DateEvaluationRule extends EvaluationRule<Movie, DefaultSchemaElement> {

	@Override
	public boolean isEqual(Movie record1, Movie record2, DefaultSchemaElement schemaElement) {
		if(record1.getDate()==null && record2.getDate()==null)
			return true;
		else if(record1.getDate()==null ^ record2.getDate()==null)
			return false;
		else
			return record1.getDate().getYear() == record2.getDate().getYear();
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.EvaluationRule#isEqual(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@Override
	public boolean isEqual(Movie record1, Movie record2,
			Correspondence<DefaultSchemaElement, Movie> schemaCorrespondence) {
		return isEqual(record1, record2, (DefaultSchemaElement)null);
	}
	
}
