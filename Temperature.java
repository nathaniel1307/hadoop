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
import java.lang.Math;

public class Temperature {
    public static class TempMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private IntWritable val = new IntWritable();
		private Text text = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String Line = value.toString();
			String[] row = Line.split(",");
            
            
			if(row[0].equals("UK000056225") || row[0].equals("UK000003377")){
				if(row[2].equals("TMAX") || row[2].equals("TMIN")){
					text.set(row[0] + "-" + row[1]);
					val.set(Integer.parseInt(row[3]));
					context.write(text, val);
				}
			}
		}
	}
    
	public static class TempReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int tempMax = values.iterator().next().get();
			if(values.iterator().hasNext()){
                IntWritable tempDif = new IntWritable();
				tempDif.set(Math.abs(tempMax - values.iterator().next().get()));
				
				context.write(key, tempDif);
			}			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Temperature.class);
		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}