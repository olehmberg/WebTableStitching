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

import java.io.Serializable;
import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public interface SparkCollection<RecordType> extends Serializable{
	
	public void parallelize();
	
	boolean isDistributed();
	
	JavaRDD<RecordType> getDistributedData();

	public Collection<RecordType> get();

	public int size();
	
}
