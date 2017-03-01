package de.uni_mannheim.informatik.dws.t2k.utils.data.sources;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.mit.jwi.Dictionary;
import edu.mit.jwi.IDictionary;
import edu.mit.jwi.item.IIndexWord;
import edu.mit.jwi.item.ISynset;
import edu.mit.jwi.item.ISynsetID;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.IWordID;
import edu.mit.jwi.item.POS;
import edu.mit.jwi.item.Pointer;

public class WordnetAPI {
	static String WN_HOME = "C:\\Program Files (x86)\\WordNet\\2.1\\dict";

	private static WordnetAPI service;
	private IDictionary dict;

	public synchronized static WordnetAPI getInstance(String wordnetPath) {
		if (service == null) {
			try {
				WN_HOME = wordnetPath;
				service = new WordnetAPI();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return service;
	}

	private WordnetAPI() throws Exception {
		URL url = new URL("file", null, WN_HOME);
		// construct the dictionary object and open it
		dict = new Dictionary(url);
		dict.open();

	}

	public List<String> getSynonyms(String wordStr) {
		// System.out.println("geting synonyms: " + wordStr);
		//long start = System.currentTimeMillis();
		List<String> synonyms = new ArrayList<String>();
		IIndexWord idxWord = dict.getIndexWord(wordStr, POS.NOUN);
		if (idxWord == null || idxWord.getWordIDs() == null
				|| idxWord.getWordIDs().size() == 0)
			return synonyms;
		IWordID wordID = idxWord.getWordIDs().get(0); // 1st meaning
		IWord word = dict.getWord(wordID);
		ISynset synset = word.getSynset();

		// iterate over words associated with the synset
		for (IWord w : synset.getWords())
			synonyms.add(w.getLemma().replaceAll("_", " ").replaceAll("-", " "));
		// System.out.println("returned synonyms "
		// + (System.currentTimeMillis() - start));
		return synonyms;
	}

	public List<String> getHypernyms(String wordStr) {
		// System.out.println("geting hypernyms: " + wordStr);
		//long start = System.currentTimeMillis();
		List<String> hypernymsStr = new ArrayList<String>();
		// get the synset
		IIndexWord idxWord = dict.getIndexWord(wordStr, POS.NOUN);
		if (idxWord == null || idxWord.getWordIDs() == null
				|| idxWord.getWordIDs().size() == 0)
			return hypernymsStr;
		IWordID wordID = idxWord.getWordIDs().get(0); // 1st meaning
		IWord word = dict.getWord(wordID);
		ISynset synset = word.getSynset();

		// get the hypernyms
		List<ISynsetID> hypernyms = synset.getRelatedSynsets(Pointer.HYPERNYM);

		// print out each h y p e r n y m s id and synonyms
		List<IWord> words;
		for (ISynsetID sid : hypernyms) {
			words = dict.getSynset(sid).getWords();

			for (Iterator<IWord> i = words.iterator(); i.hasNext();) {
				hypernymsStr.add(i.next().getLemma().replaceAll("_", " ")
						.replaceAll("-", " "));
			}

		}
		// System.out.println("returned hypernyms "
		// + (System.currentTimeMillis() - start));
		return hypernymsStr;
	}

	public List<String> getHyponyms(String wordStr) {
		// System.out.println("geting hyponyms: " + wordStr);
		//long start = System.currentTimeMillis();
		List<String> hyponymsStr = new ArrayList<String>();
		// get the synset
		IIndexWord idxWord = dict.getIndexWord(wordStr, POS.NOUN);
		if (idxWord == null || idxWord.getWordIDs() == null
				|| idxWord.getWordIDs().size() == 0)
			return hyponymsStr;
		IWordID wordID = idxWord.getWordIDs().get(0); // 1st meaning
		IWord word = dict.getWord(wordID);
		ISynset synset = word.getSynset();

		// get the hyponyms
		List<ISynsetID> hyponyms = synset.getRelatedSynsets(Pointer.HYPONYM);

		// print out each hyponym id and synonyms
		List<IWord> words;
		for (ISynsetID sid : hyponyms) {
			words = dict.getSynset(sid).getWords();

			for (Iterator<IWord> i = words.iterator(); i.hasNext();) {
				hyponymsStr.add(i.next().getLemma().replaceAll("_", " ")
						.replaceAll("-", " "));
			}

		}
		// System.out.println("returned hyponyms "
		// + (System.currentTimeMillis() - start));
		return hyponymsStr;
	}

	public static void main(String[] args) {

		WordnetAPI api = WordnetAPI.getInstance(WN_HOME);
		List<String> sys = api.getHyponyms("cat");
		for (String str : sys)
			System.out.println(str);
		// StandardAnalyzer an = new StandardAnalyzer(Version.LUCENE_45);
		// an.getStopwordSet();
		//
		// StopAnalyzer anl = new StopAnalyzer(Version.LUCENE_45);

	}
}
