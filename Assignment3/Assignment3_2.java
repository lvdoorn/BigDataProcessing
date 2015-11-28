import java.io.IOException;
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

public class Assignment3_2 extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Assignment3_2.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Assignment3_2(), args);
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
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath() // get the name of the file
					.getName();
			String line = lineText.toString();
			Text currentWord;
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				currentWord = new Text(word);
				context.write(currentWord, new Text(filename));
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, IntWritable> {
		
		private int wordCounter = 0;
		
		@Override
		public void reduce(Text word, Iterable<Text> counts,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (Text document : counts) {
				count++;
			}
			if (count == 1) {
				wordCounter++;
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Words that appear in only one document: "), new IntWritable(wordCounter));
		}
	}
}