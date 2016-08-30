import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob.State;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.text.*;

public class MatchCount 
{
    static enum MyCounters{counter;}

    public static class REMapper extends Mapper<Object, Text, Text, IntWritable> 
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            try{
                String str = value.toString();
                String str_temp = null;
                if(str.contains("Result"))
                {
                    if(str.contains("0-1"))
                    {str_temp="Black";}
                    else if(str.contains("1-0"))
                    {str_temp="White";}
                    else if(str.contains("1/2-1/2"))
                    {str_temp="Draw";}
                    word.set(str_temp);
                    context.write(word, one);
                    context.getCounter(MyCounters.counter).increment(1);
                }
            }catch(Exception e)
            {e.printStackTrace();}
        }
    }

    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
            for (IntWritable val : values) 
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, Text> 
    {
        DecimalFormat df = new DecimalFormat("#.##");
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
        {
            String s = context.getConfiguration().get("sum");
            int count= Integer.parseInt(s);
            int sum = 0;
            for (IntWritable val : values) 
            {
                sum += val.get();
            }
            String avg = df.format((double)sum / count);
            context.write(key, new Text(sum+" "+avg));
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "result count1");
        job1.setJarByClass(MatchCount .class);
        job1.setMapperClass(REMapper.class);
        job1.setReducerClass(Combiner.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("Temp"));
        job1.waitForCompletion(true); 
        
        Counter counter = job1.getCounters().findCounter(MyCounters.counter);
        long tCount = counter.getValue();
        String sCount = String .valueOf(tCount);

        Configuration conf2 = new Configuration();
        conf2.set("sum",sCount);

        Job job2 = Job.getInstance(conf2, "result count2");
        job2.setJarByClass(MatchCount .class);
        job2.setMapperClass(REMapper.class);
        job2.setCombinerClass(Combiner.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}