package de.uni_mannheim.informatik.dws.t2k.index;

import java.io.Serializable;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;

public interface IIndex extends Serializable{
	IndexSearcher getIndexSearcher();
	IndexWriter getIndexWriter();
	void closeIndexWriter();
	void closeIndexReader();
	int getNmDocs();
}
