package de.uni_mannheim.informatik.dws.t2k.index.io;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import de.uni_mannheim.informatik.dws.t2k.index.IIndex;

public class InMemoryIndex implements IIndex {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private IndexWriter indexWriter = null;
	IndexSearcher indexSearcher = null;
	private IndexReader indexReader = null;
	Directory directory = null;
	
	public InMemoryIndex()
	{
		directory = new RAMDirectory();
	}
	
	public IndexSearcher getIndexSearcher() {
		if (indexSearcher == null) {

			try {
				indexReader = DirectoryReader.open(directory);
				indexSearcher = new IndexSearcher(indexReader);

			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		return this.indexSearcher;
	}

	public IndexWriter getIndexWriter() {
		if (indexWriter == null) {
			try {
				Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
				IndexWriterConfig iwc = new IndexWriterConfig(
						Version.LUCENE_46, analyzer);

				iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

				indexWriter = new IndexWriter(directory, iwc);
				// IndexWriterConfig conf = new IndexWriterConfig(
				// Version.LUCENE_46, analyzer);
				indexWriter.getConfig().setRAMBufferSizeMB(1024);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return indexWriter;
	}

	public void closeIndexWriter() {
		if (indexWriter != null) {
			try {
				indexWriter.commit();
				indexWriter.close();
				indexWriter = null;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
	
	public void closeIndexReader() {
		try {
			if (indexReader != null)
			{
				indexReader.close();
				indexReader = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getNmDocs() {
		return getIndexSearcher().getIndexReader().numDocs();
	}

}
