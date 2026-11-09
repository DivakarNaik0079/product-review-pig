import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovingAverage {
    public static class AvgMapper extends Mapper<Object, Text, Text, FloatWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] cols = value.toString().split("\t");
            if (cols.length > 7 && !cols[0].equals("marketplace")) {
                try {
                    context.write(new Text(cols[3]), new FloatWritable(Float.parseFloat(cols[7]))); // Product ID and Star Rating
                } catch (NumberFormatException e) { }
            }
        }
    }
    public static class AvgReducer extends Reducer<Text, FloatWritable, Text, Text> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            StringBuilder movingAvgs = new StringBuilder();
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
                movingAvgs.append(sum / count).append(",");
            }
            context.write(key, new Text("Moving Averages: " + movingAvgs.toString()));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Moving Average");
        job.setJarByClass(MovingAverage.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
