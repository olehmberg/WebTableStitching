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
 * A class for wrapping three objects of arbitrary type
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <TFirst>
 * @param <TSecond>
 * @param <TThird>
 */
public class Triple<TFirst, TSecond, TThird> {

	private TFirst first;
	private TSecond second;
	private TThird third;

	/**
	 * Creates a new triple with the specified objects
	 * 
	 * @param first
	 * @param second
	 * @param third
	 */
	public Triple(TFirst first, TSecond second, TThird third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}

	/**
	 * Returns the first object
	 * 
	 * @return
	 */
	public TFirst getFirst() {
		return first;
	}

	/**
	 * Sets the first object
	 * 
	 * @param first
	 */
	public void setFirst(TFirst first) {
		this.first = first;
	}

	/**
	 * Returns the second object
	 * 
	 * @return
	 */
	public TSecond getSecond() {
		return second;
	}

	/**
	 * Sets the second object
	 * 
	 * @param second
	 */
	public void setSecond(TSecond second) {
		this.second = second;
	}

	/**
	 * Returns the third object
	 * 
	 * @return
	 */
	public TThird getThird() {
		return third;
	}

	/**
	 * Sets the third object
	 * 
	 * @param third
	 */
	public void setThird(TThird third) {
		this.third = third;
	}
}
