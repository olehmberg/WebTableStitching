package de.mannheim.uni.index.io;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;

import de.uni_mannheim.informatik.dws.t2k.index.io.DefaultIndex;
import junit.framework.TestCase;

public class DefaultIndexTest extends TestCase {

    public void testIndexWriter() throws IOException {
        
        DefaultIndex idx = new DefaultIndex("test");
        
        IndexWriter w = idx.getIndexWriter();
        
        Document d = new Document();
        d.add(new StringField("field1", "value1", Store.YES));
        d.add(new StringField("field2", "value2", Store.YES));
        
        w.addDocument(d);
        
        idx.closeIndexWriter();
        
        w = idx.getIndexWriter();
        
        d = new Document();
        d.add(new StringField("field1", "value3", Store.YES));
        d.add(new StringField("field2", "value4", Store.YES));
        
        w.addDocument(d);
        
        idx.closeIndexWriter();
    }
    
}
