import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;


public class TermToIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public static final Set<String> STOP_WORDS = new HashSet<String>(Arrays.asList("a", 
			"above", "again", "all", "an", "any", "as", "be", "been", "being", "between",
			"but", "could", "do", "doing", "during", "few", "from", "had", "have", "he",
			"he'll", "her", "here's", "herself", "himself", "how", "I", "I'll", "I've",
			"in", "is", "it's", "itself", "me", "most", "myself", "of", "once", "or",
			"ought", "ours", "out", "own", "she", "she'll", "should", "some", "than",
			"that's", "their", "them", "then", "there's", "they", "they'll", "they've",
			"those", "to", "under", "up", "was", "we'd", "we're", "were", "what's", "when's",
			"where's", "while", "who's", "why", "with", "you", "you'll", "you've", "yours",
			"yourselves", "about", "after", "against", "am", "and", "are", "at", "because",
			"before", "below", "both", "by", "did", "does", "down", "each", "for", "further",
			"has", "having", "he'd", "he's", "here", "hers", "him", "his", "how's", "I'd",
			"I'm", "if", "into", "it", "its", "let's", "more", "my", "nor", "on", "only",
			"other", "our", "ourselves", "over", "same", "she'd", "she's", "so", "such", "that",
			"the", "theirs", "themselves", "there", "these", "they'd", "they're", "this",
			"through", "too", "until", "very", "we", "we'll", "we've", "what", "when", "where",
			"which", "who", "whom", "why's", "would", "you'd", "you're", "your", "yourself"));
	
	public static final Set<String> JUNK_TOKENS = new HashSet<String>(Arrays.asList("$",
			"@", "#", "&", "*", "%"));
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		//key -> identifier for bunch of documents
		//value -> every line that contains a certain doc
		//context -> the intermediate result
		
		String[] holder = value.toString().split(",");
		//holder[3] is the content, holder[0] is the doc id
		Text id = new Text(holder[0].replaceAll("\\s+", ""));
		StringTokenizer content = new StringTokenizer(holder[3]);
		while (content.hasMoreTokens()) {
			String token = content.nextToken();
			//now trim it and remove the noisy characters
			token = token.replaceAll("[^A-Za-z]+", "").toLowerCase();
			if (token.equals("")) continue;
			//if not then check if it is junk or not
			if (STOP_WORDS.contains(token) || JUNK_TOKENS.contains(token)) continue;
			
			//otherwise, it is a valid token
			Text outKey = new Text(token);
			context.write(outKey, id);
		}
	}
	
}
