package de.uni_mannheim.informatik.dws.t2k.index.io;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import de.uni_mannheim.informatik.dws.t2k.index.IIndex;

public class DefaultIndex implements IIndex {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DefaultIndex() {
		
	}
	
	private IndexWriter indexWriter = null;
	IndexSearcher indexSearcher = null;
	private String indexDir;
	private IndexReader indexReader = null;
	
	public DefaultIndex(String indexdir)
	{
		indexDir = indexdir;
	}
	
	public String getIndexDir() {
		return indexDir;
	}
	
	public IndexSearcher getIndexSearcher() {
		if (indexSearcher == null) {

			try {
				File indexDirFile = new File(this.indexDir);
				Directory dir = FSDirectory.open(indexDirFile);
				//Directory dir = MMapDirectory.open(indexDirFile);
				indexReader = DirectoryReader.open(dir);
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
				File indexDirFile = new File(this.indexDir);

				Directory dir = FSDirectory.open(indexDirFile);
				Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
				IndexWriterConfig iwc = new IndexWriterConfig(
						Version.LUCENE_46, analyzer);

				iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

				indexWriter = new IndexWriter(dir, iwc);
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
			if (indexReader != null) {
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
