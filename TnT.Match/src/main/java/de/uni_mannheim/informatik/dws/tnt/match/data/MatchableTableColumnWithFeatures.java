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
package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.wdi.model.Matchable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableColumnWithFeatures implements Matchable {
	
	private Set<List<String>> contentPatterns = new HashSet<>();
	private int minNumTokens = Integer.MAX_VALUE;
	private int maxNumTokens = Integer.MIN_VALUE;
	private Map<String, Integer> tokenFrequencies = new HashMap<>();
	private boolean isEmpty = true;
	
	private MatchableTableColumn column;
	
	/**
	 * @return the column
	 */
	public MatchableTableColumn getColumn() {
		return column;
	}
	
	public void setColumn(MatchableTableColumn column) {
		this.column = column;
	}
	
	public Set<List<String>> getContentPatterns() {
		return contentPatterns;
	}

	public void setContentPatterns(Set<List<String>> contentPatterns) {
		this.contentPatterns = contentPatterns;
	}

	public int getMinNumTokens() {
		return minNumTokens;
	}

	public void setMinNumTokens(int minNumTokens) {
		this.minNumTokens = minNumTokens;
	}

	public int getMaxNumTokens() {
		return maxNumTokens;
	}

	public void setMaxNumTokens(int maxNumTokens) {
		this.maxNumTokens = maxNumTokens;
	}

	public Map<String, Integer> getTokenFrequencies() {
		return tokenFrequencies;
	}

	public void setTokenFrequencies(Map<String, Integer> tokenFrequencies) {
		this.tokenFrequencies = tokenFrequencies;
	}

	public boolean isEmpty() {
		return isEmpty;
	}

	public void setEmpty(boolean isEmpty) {
		this.isEmpty = isEmpty;
	}

	/**
	 * 
	 */
	public MatchableTableColumnWithFeatures() {
		super();
	}
	
	public MatchableTableColumnWithFeatures(MatchableTableColumn c) {
		super();
		setColumn(c);
	}
	
	public void updateFeatures(Object value) {
		
		if(value!=null) {
			String stringValue = value.toString();
			
			String[] tokens = stringValue.split("[\\W]");
			
			if(tokens.length<minNumTokens) {
				minNumTokens = tokens.length;
			}
			if(tokens.length>maxNumTokens) {
				maxNumTokens = tokens.length;
			}
			
			//TODO token types cannot contain punctuation with this approach ... 
			List<String> tokenTypes = new LinkedList<>();
			for(String token : tokens) {
				MapUtils.increment(tokenFrequencies, token);
				tokenTypes.add(getTokenType(token));
			}
			
			Iterator<String> tokenIt = tokenTypes.iterator();
			String last = null;
			while(tokenIt.hasNext()) {
				String current = tokenIt.next();
				
				if(current.equals(last)) {
					tokenIt.remove();
				}
				
				last = current;
			}
			
			contentPatterns.add(tokenTypes);
			
			isEmpty = false;
		}
		
	}
	
	protected static final Pattern DIGIT = Pattern.compile("^\\d$");
	protected static final Pattern CHARACTER = Pattern.compile("^\\w$");
	
	protected String getTokenType(String token) {
		if(DIGIT.matcher(token).matches()) {
			return "D";
		} else if(CHARACTER.matcher(token).matches()) {
			return "C";
		} else {
			return "O";
		}
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getIdentifier()
	 */
	@Override
	public String getIdentifier() {
		return column.getIdentifier();
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getProvenance()
	 */
	@Override
	public String getProvenance() {
		return column.getProvenance();
	}
}
