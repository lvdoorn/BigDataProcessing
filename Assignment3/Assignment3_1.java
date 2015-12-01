import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Assignment3_1 extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(Assignment3_1.class);

	public enum Counters {
		UNIQUES, START_LETTER_T, FILES_READ, OCCURS_LESS_THAN_5;
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

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Assignment3_1(), args);
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

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable inf = new IntWritable(
				Integer.MAX_VALUE);
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath()
					.getName();
			String line = lineText.toString();
			Text currentWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				currentWord = new Text(word);
				context.write(currentWord, one); // emit the word
				context.write(new Text("***" + filename + "***"), inf); 
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private SortedSet<KeyValue> frequencies;
		private List<Character> startingChars;
		private Set<Text> files;

		public void setup(Context context) throws IOException {
			startingChars = new ArrayList<Character>();
			startingChars.add('T');
			startingChars.add('t');
			frequencies = new TreeSet<KeyValue>();
			files = new HashSet<Text>();
		}

		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			
			int inf = Integer.MAX_VALUE;
			boolean file = false;
			int counter = 0;
			for (IntWritable count : counts) {
				if (count.get() == inf) { 
					file = true;
					break;
				}
				counter++;
			}
			if (!file && word.toString().matches("[A-Za-z]*")) {
				context.getCounter(Counters.UNIQUES).increment(1);
				if (startingChars.contains(word.toString().charAt(0))) {
					context.getCounter(Counters.START_LETTER_T).increment(1);
				}

				if (counter < 5) {
					context.getCounter(Counters.OCCURS_LESS_THAN_5)
							.increment(1);
				}
				if (frequencies.size() < 5) {
					frequencies.add(new KeyValue(word.toString(), counter));
				} else if (frequencies.first().value < counter) {
					frequencies.add(new KeyValue(word.toString(), counter));
					frequencies.remove(frequencies.first());
				}
			} else if (file && !files.contains(word)) { 
				files.add(word);
				context.getCounter(Counters.FILES_READ).increment(1);
			}
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (KeyValue element : frequencies) {
				context.write(new Text(element.getKey()),
						new IntWritable(element.getValue()));
			}
		}
	}
}
