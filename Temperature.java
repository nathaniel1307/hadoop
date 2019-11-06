import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Temperature {
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable>{
		//private final static IntWritable one = new IntWritable(1);
		
        private IntWritable val = new IntWritable();
        private Text text = new Text();

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String Line = value.toString();
            String[] SplitLine = Line.split(",");
            
            if(SplitLine[0].equals("UK000056225") || SplitLine[0].equals("UK000003377")){
				if(SplitLine[2].equals("TMAX") || SplitLine[2].equals("TMIN")){
					text.set(SplitLine[0] + "-" + SplitLine[1] + "-" + SplitLine[2]);
					val.set(Integer.parseInt(SplitLine[3]));
					context.write(text, val);
				}
				
			}


		}
	} 
	public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// int sum = 0;
			// for (IntWritable val : values) {
			// 	sum += val.get();
			// }
            // result.set(sum);
            values.iterator.next();
			context.write(key, null);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Temperature.class);
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}