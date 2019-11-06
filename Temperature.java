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
        
        private Text key = new Text();
        private IntWritable value = new IntWritable();

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] record = line.split(",");
            
            if(record[0].equals("UK000056225") || record[0].equals("UK000003377")){
				if(record[2].equals("TMAX") || record[2].equals("TMIN")){
					key.set(record[0] + "-" + record[1] + "-" + record[2]);
					value.set(Integer.parseInt(record[3]));
					context.write(key, value);
				}
			}
		}
    } 
    
	public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
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