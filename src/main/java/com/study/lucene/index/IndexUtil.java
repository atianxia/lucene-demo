package com.study.lucene.index;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;


public class IndexUtil {
	private String[] ids = {"1","2","3","4","5","6"};
	private String[] emails = {"aa@itat.org","bb@itat.org","cc@cc.org","dd@sina.org","ee@zttc.edu","ff@itat.org"};
	private String[] contents = {
			"welcome to visited the space,I like book",
			"hello boy, I like pingpeng ball",
			"my name is cc I like game",
			"I like football",
			"I like football and I like basketball too",
			"I like movie and swim"
	};
	private Date[] dates = null;
	private int[] attachs = {2,3,1,4,5,5};
	private String[] names = {"zhangsan","lisi","john","jetty","mike","jake"};
	private Directory directory = null;
	private Map<String,Float> scores = new HashMap<String,Float>();
	private static DirectoryReader reader = null;
	
	public IndexUtil() {
		try {
			setDates();
			scores.put("itat.org",2.0f);
			scores.put("zttc.edu", 1.5f);
			directory = FSDirectory.open(Paths.get("d:/lucene/index021"));
//			directory = new RAMDirectory();
			index();
			reader = DirectoryReader.open(directory);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public IndexSearcher getSearcher() {
		try {
			if(reader==null) {
				reader = DirectoryReader.open(directory);
			} else {
				IndexReader tr = DirectoryReader.openIfChanged(reader);
				if(tr!=null) {
					reader.close();
					reader = (DirectoryReader)tr;
				}
			}
			return new IndexSearcher(reader);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	private void setDates() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			dates = new Date[ids.length];
			dates[0] = sdf.parse("2010-02-19");
			dates[1] = sdf.parse("2012-01-11");
			dates[2] = sdf.parse("2011-09-19");
			dates[3] = sdf.parse("2010-12-22");
			dates[4] = sdf.parse("2012-01-01");
			dates[5] = sdf.parse("2011-05-19");
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public void undelete() {
		try {
			IndexWriter writer = new IndexWriter(directory,
					new IndexWriterConfig(new StandardAnalyzer()));
			writer.rollback();
			writer.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void merge() {
		IndexWriter writer = null;
		try {
			writer = new IndexWriter(directory,
					new IndexWriterConfig(new StandardAnalyzer()));
			writer.forceMerge(2);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(writer!=null) writer.close();
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void forceDelete() {
		IndexWriter writer = null;
		
		try {
			writer = new IndexWriter(directory,
					new IndexWriterConfig(new StandardAnalyzer()));
			writer.forceMergeDeletes();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(writer!=null) writer.close();
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void delete() {
		IndexWriter writer = null;
		
		try {
			writer = new IndexWriter(directory,
					new IndexWriterConfig(new StandardAnalyzer()));
			writer.deleteDocuments(new Term("id","1"));
//			writer.commit();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(writer!=null) writer.close();
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void delete02() {
		try(IndexWriter writer = new IndexWriter(directory,
				new IndexWriterConfig(new StandardAnalyzer()))) {
			writer.deleteDocuments(new Term("id","1"));
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void update() {
		IndexWriter writer = null;
		try {
			writer = new IndexWriter(directory,
					new IndexWriterConfig(new StandardAnalyzer()));
			Document doc = new Document();
			doc.add(new StringField("id","11",Field.Store.YES));
			doc.add(new StringField("email",emails[0],Field.Store.YES));
			doc.add(new TextField("content",contents[0],Field.Store.NO));
			doc.add(new StringField("name",names[0],Field.Store.YES));
			writer.updateDocument(new Term("id","1"), doc);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(writer!=null) writer.close();
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void query() {
		try {
			System.out.println("numDocs:"+reader.numDocs());
			System.out.println("maxDocs:"+reader.maxDoc());
			System.out.println("deleteDocs:"+reader.numDeletedDocs());
			reader.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void index() {
		IndexWriter writer = null;
		try {
			writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()));
			writer.deleteAll();
			Document doc = null;
			for(int i=0;i<ids.length;i++) {
				doc = new Document();
				doc.add(new StringField("id",ids[i],Field.Store.YES));
				doc.add(new StringField("email",emails[i],Field.Store.YES));
				doc.add(new StringField("email","test"+i+"@test.com",Field.Store.YES));
				doc.add(new TextField("content",contents[i],Field.Store.NO));
				doc.add(new StringField("name",names[i],Field.Store.YES));
				doc.add(new IntPoint("attach",attachs[i]));
				doc.add(new LongPoint("date",dates[i].getTime()));
				String et = emails[i].substring(emails[i].lastIndexOf("@")+1);
				System.out.println(et);
				/*if(scores.containsKey(et)) {
					doc.setBoost(scores.get(et));
				} else {
					doc.setBoost(0.5f);
				}*/
				writer.addDocument(doc);
			}
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(writer!=null)writer.close();
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void search01() {
		try {
			IndexSearcher searcher = new IndexSearcher(reader);
			TermQuery query = new TermQuery(new Term("email","test0@test.com"));
			TopDocs tds = searcher.search(query, 10);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println("("+sd.doc+"-"+"-"+sd.score+")"+
						doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
						doc.get("attach")+","+doc.get("date")+","+doc.getValues("email")[1]);
			}
			reader.close();
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void search02() {
		try {
			IndexSearcher searcher = getSearcher();
			TermQuery query = new TermQuery(new Term("content","like"));
			TopDocs tds = searcher.search(query, 10);
			for(ScoreDoc sd:tds.scoreDocs) {
				Document doc = searcher.doc(sd.doc);
				System.out.println(doc.get("id")+"---->"+
						doc.get("name")+"["+doc.get("email")+"]-->"+doc.get("id")+","+
						doc.get("attach")+","+doc.get("date")+","+doc.getValues("email")[1]);
			}
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
