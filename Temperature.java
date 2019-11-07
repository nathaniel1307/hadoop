import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.lang.Math;

public class Temperature {
    public static class TempMapper extends Mapper<Object, Text, Text, FloatWritable>{
		
		private FloatWritable val = new FloatWritable();
		private Text text = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] rows = (value.toString()).split(",");
            float floatValue = (float) (Integer.parseInt(rows[3]));


			if(rows[0].equals("UK000056225")){
				if(rows[2].equals("TMAX") || rows[2].equals("TMIN")){
					text.set("Oxford_" + rows[1].substring(6) + "/" + rows[1].substring(4,6) + "/" + rows[1].substring(0, 4));
					val.set(floatValue);
					context.write(text, val);
				}
			}else if(rows[0].equals("UK000003377")){
                if(rows[2].equals("TMAX") || rows[2].equals("TMIN")){
					text.set("Waddinton_" + rows[1].substring(6) + "/" + rows[1].substring(4,6) + "/" + rows[1].substring(0, 4));
					val.set(floatValue);
					context.write(text, val);
				}
            }
		}
	}
    
	public static class TempReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			
			float tempMax = values.iterator().next().get();
			if(values.iterator().hasNext()){
                FloatWritable tempDifference = new FloatWritable();
                tempDifference.set((Math.abs(tempMax - values.iterator().next().get()))/10);
				context.write(key, tempDifference);
			}			
		}
	}

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "Temperature");
		job.setJarByClass(Temperature.class);
		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}