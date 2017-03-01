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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.uni_mannheim.informatik.wdi.model.BasicCollection;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SparkBasicCollectionWrapper<RecordType> implements BasicCollection<RecordType>, SparkCollection<RecordType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JavaRDD<RecordType> distributedData = null;
	private JavaSparkContext sparkContext = null;
	
	private BasicCollection<RecordType> actualCollection = null;
	
	public SparkBasicCollectionWrapper(JavaSparkContext sc) {
		super();
		sparkContext = sc;
		actualCollection = this;
	}
	
	public SparkBasicCollectionWrapper(JavaSparkContext sc, BasicCollection<RecordType> collection) {
		super();
		sparkContext = sc;
		actualCollection = collection;
	}
	
	public void parallelize() {
		if(!isDistributed()) {
			distributedData = sparkContext.parallelize(new ArrayList<>(actualCollection.get()));
		}
	}
	
	public boolean isDistributed() {
		return distributedData != null;
	}
	
	public JavaRDD<RecordType> getDistributedData() {
		return distributedData;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.BasicCollection#add(java.lang.Object)
	 */
	@Override
	public void add(RecordType element) {
		actualCollection.add(element);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.BasicCollection#get()
	 */
	@Override
	public Collection<RecordType> get() {
		return actualCollection.get();
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.BasicCollection#size()
	 */
	@Override
	public int size() {
		return actualCollection.size();
	}

}
