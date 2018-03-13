import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertIndexReducer extends Reducer<Text, Text, Text, Text> { 
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		//key -> a certain token
		//values -> all the text objects that matches to a this token
		//context -> the result
		
		Set<String> uniques = new HashSet<String>();
		StringBuilder result = new StringBuilder();
		
		for (Text value: values) {
			uniques.add(value.toString());
		}
		//then add items to result
		String prefix = "";
		for (String item: uniques) {
			if (item.equals("")) continue;
			result.append(prefix);
			prefix = ", ";
			result.append(item);
		}
		//write out the result
		Text value = new Text(result.toString());
		Text outKey = new Text(key.toString() + " => ");
		context.write(outKey, value);
	}
}
