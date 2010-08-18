package edu.wiki.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.CustomTokenizer;
import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;


public class WikipediaAnalyzer extends Analyzer {
	
	/** An unmodifiable set containing some common English words that are not usually useful
	  for searching.*/
	public final Set<?> ENGLISH_STOP_WORDS_SET;
	
	public WikipediaAnalyzer() throws IOException {
		// read stop words
		InputStream is = ESAWikipediaIndexer.class.getResourceAsStream("/config/stopwords.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		ArrayList<String> stopWords = new ArrayList<String>(500);
		
		String line;
		
		while((line = br.readLine()) != null){
			stopWords.add(line);
		}
		
		br.close();
		
		final CharArraySet stopSet = new CharArraySet(stopWords.size(), false);
		stopSet.addAll(stopWords);  
				
		ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet);

	}

	
    public TokenStream reusableTokenStream(
        String fieldName, Reader reader) throws IOException {

        SavedStreams streams =
            (SavedStreams) getPreviousTokenStream();

        if (streams == null) {
            streams = new SavedStreams();
            setPreviousTokenStream(streams);

            // streams.tokenizer = new LetterTokenizer(reader);
            streams.tokenizer = new CustomTokenizer(reader);
            
            streams.stream = new StandardFilter(streams.tokenizer);
            streams.stream = new LengthFilter(streams.stream, 3, 100);
            streams.stream = new LowerCaseFilter(streams.stream);
            // streams.stream = new StopFilter(true, streams.stream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
            streams.stream = new StopFilter(true, streams.stream, ENGLISH_STOP_WORDS_SET);
            streams.stream = new PorterStemFilter(streams.stream);
        } else {
            streams.tokenizer.reset(reader);
        }

        return streams.stream;
    }

    private class SavedStreams {
        Tokenizer tokenizer;
        TokenStream stream;
    }

    public TokenStream tokenStream(
        String fieldName, Reader reader) {

        // Tokenizer tokenizer = new LetterTokenizer(reader);
    	Tokenizer tokenizer = new CustomTokenizer(reader);

        TokenStream stream = new StandardFilter(tokenizer);
        stream = new LengthFilter(stream, 3, 100);
        stream = new LowerCaseFilter(stream);
        // stream = new StopFilter(true, stream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        stream = new StopFilter(true, stream, ENGLISH_STOP_WORDS_SET);
        stream = new PorterStemFilter(stream);

        return stream;
    }
}