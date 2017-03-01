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
package de.uni_mannheim.informatik.wdi.spark.processing;

import org.apache.spark.api.java.function.Function;

import de.uni_mannheim.informatik.wdi.model.Pair;
import scala.Tuple2;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SparkZipWithUniqueIdAssignmentWrapper<RecordType> implements Function<Tuple2<RecordType, Long>, RecordType> {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private de.uni_mannheim.informatik.wdi.processing.Function<RecordType, Pair<Long, RecordType>> wrappedFunction;

	public SparkZipWithUniqueIdAssignmentWrapper(de.uni_mannheim.informatik.wdi.processing.Function<RecordType, Pair<Long, RecordType>> wrappedFunction) {
		this.wrappedFunction = wrappedFunction;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	@Override
	public RecordType call(Tuple2<RecordType, Long> arg0) throws Exception {
		return wrappedFunction.execute(new Pair<Long, RecordType>(arg0._2(), arg0._1()));
	}
	
	
	
}
