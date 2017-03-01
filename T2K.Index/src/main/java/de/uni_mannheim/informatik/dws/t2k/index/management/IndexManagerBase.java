package de.uni_mannheim.informatik.dws.t2k.index.management;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.Version;

import de.uni_mannheim.informatik.dws.t2k.index.IIndex;

public class IndexManagerBase implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public IndexManagerBase() {
		
	}
	public IndexManagerBase(IIndex index, String defaultField) {
		this.index = index;
		this.defaultField = defaultField;
	}

	QueryParser queryParser = null;
	private IIndex index;
	private String defaultField;
	private int maxEditDistance = 0;
	private boolean searchExactMatches = false;
	private int numRetrievedDocsFromIndex = 10000;
	private Collection<String> filterValues;
	private String filterField;
	
	public String getFilterField() {
		return filterField;
	}
	
	public void setFilterField(String filterField) {
		this.filterField = filterField;
	}
	
	public Collection<String> getFilterValues() {
		return filterValues;
	}
	
	public void setFilterValues(Collection<String> filterValues) {
		this.filterValues = filterValues;
	}
	
	public int getMaxEditDistance() {
		return maxEditDistance;
	}
	
	public void setMaxEditDistance(int maxEditDistance) {
		this.maxEditDistance = maxEditDistance;
	}
	
	public boolean isSearchExactMatches() {
		return searchExactMatches;
	}
	
	public void setSearchExactMatches(boolean searchExactMatches) {
		this.searchExactMatches = searchExactMatches;
	}
	
	public int getNumRetrievedDocsFromIndex() {
		return numRetrievedDocsFromIndex;
	}
	
	public void setNumRetrievedDocsFromIndex(int numRetrievedDocsFromIndex) {
		this.numRetrievedDocsFromIndex = numRetrievedDocsFromIndex;
	}
	
	public String getDefaultField() {
		return defaultField;
	}
	
	public IIndex getIndex()
	{
		return index;
	}
	
	private static ConcurrentHashMap<Thread, QueryParser> queryParserCache = new ConcurrentHashMap<Thread, QueryParser>();
	
	protected QueryParser getQueryParser() {
		if(queryParser == null)
			queryParser = getNewQueryParser();
		return queryParser;
	}
	
	protected QueryParser getNewQueryParser()
	{
		return new QueryParser(Version.LUCENE_46,
				defaultField, new StandardAnalyzer(
						Version.LUCENE_46));
	}

	protected QueryParser getQueryParserFromCache()
	{
		QueryParser queryParser = null;
		if(!queryParserCache.containsKey(Thread.currentThread()))
		{
			queryParser = getNewQueryParser();
			queryParserCache.put(Thread.currentThread(), queryParser);
		}
		else
			queryParser = queryParserCache.get(Thread.currentThread());
		
		return queryParser;
	}
}
