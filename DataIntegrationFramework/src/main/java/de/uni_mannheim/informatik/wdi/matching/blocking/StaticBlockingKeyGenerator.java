/**
 * 
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim (code@dwslab.de)
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
package de.uni_mannheim.informatik.wdi.matching.blocking;

import de.uni_mannheim.informatik.wdi.model.Matchable;

/**
 * Implementation of a {@link BlockingKeyGenerator} which assigns to all given
 * records, always the same static key. Which means that a {@link Blocker}
 * making use of this {@link BlockingKeyGenerator} will not do any sophisticated
 * blocking.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class StaticBlockingKeyGenerator<RecordType extends Matchable> extends
		BlockingKeyGenerator<RecordType> {

	private static final long serialVersionUID = 1L;
	/**
	 * Could be anything
	 */
	private static final String STATIC_BLOCKING_KEY = "AAA";

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.uni_mannheim.informatik.wdi.identityresolution.blocking.
	 * BlockingKeyGenerator
	 * #getBlockingKey(de.uni_mannheim.informatik.wdi.model.Matchable)
	 */
	@Override
	public String getBlockingKey(RecordType recordType) {
		return STATIC_BLOCKING_KEY;
	}

}
