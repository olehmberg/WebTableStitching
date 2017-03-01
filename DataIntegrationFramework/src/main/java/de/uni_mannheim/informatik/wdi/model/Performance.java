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
package de.uni_mannheim.informatik.wdi.model;

/**
 * Contains the evaluation performance.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class Performance {

	private int correct;
	private int created;
	private int correct_total;

	public Performance(int correct, int created, int correct_total) {
		this.correct = correct;
		this.created = created;
		this.correct_total = correct_total;
	}

	/**
	 * Returns the Precision
	 * 
	 * @return
	 */
	public double getPrecision() {
		if(created==0) {
			return 0.0;
		} else {
			return (double) correct / (double) created;
		}
	}

	/**
	 * Returns the Recall
	 * 
	 * @return
	 */
	public double getRecall() {
		if(correct_total==0) {
			return 0.0;
		} else {
			return (double) correct / (double) correct_total;
		}
	}

	/**
	 * Returns the F1-Measure
	 * 
	 * @return
	 */
	public double getF1() {
		if(getPrecision()==0 || getRecall()==0) {
			return 0.0;
		} else {
			return (2 * getPrecision() * getRecall())
				/ (getPrecision() + getRecall());
		}
	}

	public int getNumberOfPredicted() {
		return created;
	}
	
	public int getNumberOfCorrectlyPredicted() {
		return correct;
	}
	
	public int getNumberOfCorrectTotal() {
		return correct_total;
	}
}
