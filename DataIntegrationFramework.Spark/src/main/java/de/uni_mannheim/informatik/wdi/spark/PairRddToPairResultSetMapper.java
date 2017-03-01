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
package de.uni_mannheim.informatik.wdi.spark;

import org.apache.spark.api.java.function.Function;

import de.uni_mannheim.informatik.wdi.model.Pair;
import scala.Tuple2;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class PairRddToPairResultSetMapper<TFirst, TSecond> implements Function<Tuple2<TFirst,TSecond>, Pair<TFirst, TSecond>> {

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Pair<TFirst, TSecond> call(Tuple2<TFirst, TSecond> arg0) throws Exception {
		return new Pair<>(arg0._1(), arg0._2());
	}

}
