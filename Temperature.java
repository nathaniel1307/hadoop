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
    //Mapper Class
    public static class TempMapper extends Mapper<Object, Text, Text, FloatWritable>{	
        //Define key and value field variables for context
		private FloatWritable val = new FloatWritable();
        private Text text = new Text();

        //Map Function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Split input into a string array, rows
            String[] rows = (value.toString()).split(",");
            //Define float for the temperature values
            float floatValue = (float) (Integer.parseInt(rows[3]));
            
            //Filter out the required locations
			if(rows[0].equals("UK000056225")){
                //Check for TMAX or TMIN
				if(rows[2].equals("TMAX") || rows[2].equals("TMIN")){
                    //Set the key value pair for the context
					text.set("Oxford_" + rows[1].substring(6) + "/" + rows[1].substring(4,6) + "/" + rows[1].substring(0, 4));
                    val.set(floatValue);
                    //Write to context
					context.write(text, val);
                }
            //Filter out the required locations
			}else if(rows[0].equals("UK000003377")){
                //Check for TMAX or TMIN
                if(rows[2].equals("TMAX") || rows[2].equals("TMIN")){
                    //Set the key value pair for the context
					text.set("Waddinton_" + rows[1].substring(6) + "/" + rows[1].substring(4,6) + "/" + rows[1].substring(0, 4));
                    val.set(floatValue);
                    //Write to context
					context.write(text, val);
				}
            }
		}
	}
    
    //Reducer Class
	public static class TempReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        //Reducer Function
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			//Get the max temperature values
            float tempMax = values.iterator().next().get();
            //Check if there is a minimum temperature
			if(values.iterator().hasNext()){
                //Define tempDifference FloatWritable
                FloatWritable tempDifference = new FloatWritable();
                //Work out the temperature difference
                tempDifference.set((Math.abs(tempMax - values.iterator().next().get()))/10);
                //Write to context
				context.write(key, tempDifference);
			}			
		}
	}

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //Set the seperator to be a comma for CSV
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