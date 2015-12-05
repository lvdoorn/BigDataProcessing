import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.Arrays;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Assignment3_3 extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Assignment3_3.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Assignment3_3(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " wordcount ");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * Nested class to make our SortedSet sort by values.
	 * 
	 */
	public static class KeyValue implements Comparable<KeyValue> {

		private String key;
		private Integer value;

		public KeyValue(String key, int value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return key;
		}

		public int getValue() {
			return value;
		}

		@Override
		public int compareTo(KeyValue o) {
			return value.compareTo(o.value);
		}
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\W+");
		private String[] words = { "the", "and", "of", "The", "And", "Of" };
		private List<String> stopWords = Arrays.asList(words);

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			String[] splitline = WORD_BOUNDARY.split(line);
			for (int i = 0; i < splitline.length - 1; i++) {
				if (stopWords.contains(splitline[i])) {
					String res = splitline[i].toLowerCase() + " " + splitline[i + 1].toLowerCase();
					context.write(new Text(res), one);
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private String[] words = { "the", "and", "of" };
		private List<String> stopWords = Arrays.asList(words);
		private SortedSet<KeyValue> theSet = new TreeSet<KeyValue>();
		private SortedSet<KeyValue> andSet = new TreeSet<KeyValue>();
		private SortedSet<KeyValue> ofSet = new TreeSet<KeyValue>();

		@Override
		public void setup(Context context) {

		}

		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			int counter = 0;
			for (IntWritable count : counts) {
				counter += count.get();
			}
			String[] input = word.toString().split(" ");
			String firstWord = input[0];
			String secondWord = input[1];
			SortedSet<KeyValue> currentSet = new TreeSet<KeyValue>();
			if (!stopWords.contains(firstWord)) {
				throw new NoSuchElementException("Not a stop word");
			}
			switch (firstWord) {
			case ("the"):
				currentSet = theSet;
				break;
			case ("and"):
				currentSet = andSet;
			break;
			case ("of"):
				currentSet = ofSet;
				break;	
			default:
				break;
			}
			if (currentSet.size() < 5) {
				currentSet.add(new KeyValue(secondWord, counter));
			} else if (currentSet.first().value < counter) {
				currentSet.add(new KeyValue(secondWord, counter));
				currentSet.remove(currentSet.first());
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (KeyValue kv : theSet) {
				context.write(new Text("the " + kv.getKey()),
						new IntWritable(kv.getValue()));
			}
			for (KeyValue kv : andSet) {
				context.write(new Text("and " + kv.getKey()),
						new IntWritable(kv.getValue()));
			}
			for (KeyValue kv : ofSet) {
				context.write(new Text("of " + kv.getKey()),
						new IntWritable(kv.getValue()));
			}
		}
	}
}
