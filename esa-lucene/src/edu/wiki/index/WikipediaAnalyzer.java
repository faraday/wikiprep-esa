package edu.wiki.index;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
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
	public static final Set<?> ENGLISH_STOP_WORDS_SET;

	static {
		final List<String> stopWords = Arrays.asList(	
				"http","www","a","about","after","all","almost","along","already","also","although","always",
				"am","among","an","and","any","another","anyone","anything","anywhere","are","around","as","at","away",
				"back","be","because","between","beyond","both","but","by","can","cannot","come","could","d","did",
				"do","does","doing","done","during","each","either","enough","etc","even","ever","every",
				"everyone","everything","few","first","five","for","four","from","further","gave","get","getting",
				"give","given","giving","go","goes","going","gone","good","got","great","had","has","have",
				"having","here","high","how","however","i","if","ii","in","includes","including","indeed",
				"instead","into","is","it","its","itself","just","less","least","like","likely","ll","long",
				"m","made","make","many","may","maybe","me","might","mine","more","most","much","must","less",
				"neither","never","new","no","nobody","noone","nor","not","nothing","now","of","old","on",
				"once","one","only","or","other","others","our","ours","out","over","own","perhaps","put",
				"rather","really","s","same","shall","said","say","says","second","see","seen","should","simply",
				"since","so","some","someone","something","sometimes","somewhere","soon","still","such","take",
				"than","that","the","their","theirs","then","there","therefore","these","they","thing","things",
				"this","those","though","through","three","thus","to","together","too","toward","two","unless",
				"until","upon","us","use","used","uses","using","usually","very","was","way","ways","we","well",
				"went","were","what","when","whenever","where","wherever","whether","which","while","who",
				"whoever","why","will","with","within","without","would","yes","yet","you","your","yours","yourself"
		);
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

            streams.tokenizer = new LetterTokenizer(reader);
            
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

        Tokenizer tokenizer = new LetterTokenizer(reader);

        TokenStream stream = new StandardFilter(tokenizer);
        stream = new LengthFilter(stream, 3, 100);
        stream = new LowerCaseFilter(stream);
        // stream = new StopFilter(true, stream, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        stream = new StopFilter(true, stream, ENGLISH_STOP_WORDS_SET);
        stream = new PorterStemFilter(stream);

        return stream;
    }
}