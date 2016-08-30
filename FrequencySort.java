import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob.State;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.DecimalFormat;


public class FrequencySort 
{
    static enum MyCounters{counter;}
    public static class ReMapper extends Mapper<Object, Text, Text, IntWritable> 
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException 
        {
            try{
                String str = value.toString();
                if(str.contains("PlyCount"))
                {
                    String step = str.trim().replace("PlyCount ", "");
                    step=step.replaceAll("\"", "");
                    step=step.replaceAll("\\[", "");
                    step=step.replaceAll("\\]", "");
                    word.set(step);
                    context.write(word, one);
                    context.getCounter(MyCounters.counter).increment(1);
                }
            }catch(Exception e)
            {e.printStackTrace();}
        }
    }

    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        DecimalFormat df = new DecimalFormat("#.####");
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
        {
            String s = context.getConfiguration().get("sum");
            int count= Integer.parseInt(s);
            int sum = 0;
            for (IntWritable value : values) 
            {
                sum+=value.get();
            }
            String result = df.format((double)sum/count*100);
            System.out.println(key.toString() + " "+result);
            context.write(key, new Text(result+"%"));
        }
    }

    public static class MyMapper extends Mapper<Object, Text, CompositeKey, Text> 
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] str = value.toString().split("\t");
            context.write(new CompositeKey(str[0], str[1]), new Text(str[1]));
        }
    }
    public static class MyReducer extends Reducer<CompositeKey, Text, Text, Text> 
    {
        public void reduce(CompositeKey key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
        {
            Text frequency = values.iterator().next();
            context.write(new Text(key.getPlyCountNum()), frequency);
        }
    }
    public static class ActualKeyPartitioner extends Partitioner<CompositeKey, Text> {

        HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<Text, Text>();
        Text newKey = new Text();

        @Override
        public int getPartition(CompositeKey compositeKey, Text text, int i) {
            try {
                newKey.set(compositeKey.getPlyCountNum());
                return hashPartitioner.getPartition(newKey, text, i);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return 0;
        }
    }
    public static class ActualKeyGroupingComparator extends WritableComparator 
    {

        public ActualKeyGroupingComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey key1 = (CompositeKey) a;
            CompositeKey key2 = (CompositeKey) b;
            return key2.getFrequency().compareTo(key1.getFrequency());
        }
    }

    public static class CompositeKeyComparator extends WritableComparator {

        protected CompositeKeyComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey key1 = (CompositeKey) a;
            CompositeKey key2 = (CompositeKey) b;
            return key2.getFrequency().compareTo(key1.getFrequency());
        }
    }
    public static class CompositeKey implements WritableComparable 
     {
        private String plyCountNum;
        private String frequency;

        public CompositeKey() 
        {super();}

        public CompositeKey(String plyCountNum, String frequency) 
        {
            super();
            this.plyCountNum = plyCountNum;
            this.frequency = frequency;
        }

        public int compareTo(Object o) 
        {
            CompositeKey key = (CompositeKey) o;
            return frequency.compareTo(key.getFrequency());
        }

        public void write(DataOutput dataOutput) throws IOException 
        {
            WritableUtils.writeString(dataOutput, plyCountNum);
            WritableUtils.writeString(dataOutput, frequency);
        }

        public void readFields(DataInput dataInput) throws IOException 
        {
            plyCountNum = WritableUtils.readString(dataInput);
            frequency = WritableUtils.readString(dataInput);
        }

        public String getPlyCountNum() 
        {
            return plyCountNum;
        }
        public void setPlyCountNum(String plyCountNum) 
        {
            this.plyCountNum = plyCountNum;
        }
        public String getFrequency()
        {
            return frequency;
        }
        public void setFrequency(String frequency) 
        {
            this.frequency = frequency;
        }
    }

    public static void main(String[] args) throws Exception 
    {
        String pathtemp="/Users/hadoop/tempout";
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Sum");
        job1.setJarByClass(FrequencySort.class);
        job1.setMapperClass(ReMapper.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp11"));
        job1.waitForCompletion(true); 

        Counter counter = job1.getCounters().findCounter(MyCounters.counter);
        long tCount = counter.getValue();
        String sCount = String .valueOf(tCount);

        Configuration conf2 = new Configuration();
        conf2.set("sum", sCount);

        Job job2 = Job.getInstance(conf2, "result count");
        job2.setJarByClass(FrequencySort.class);
        job2.setMapperClass(ReMapper.class);
        job2.setCombinerClass(Combiner.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(pathtemp));
        job2.waitForCompletion(true); 

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "result count");
        job3.setJarByClass(FrequencySort.class);
        job3.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
        job3.setPartitionerClass(ActualKeyPartitioner.class);
        job3.setSortComparatorClass(CompositeKeyComparator.class);
        job3.setMapperClass(MyMapper.class);
        job3.setReducerClass(MyReducer.class);
        job3.setMapOutputKeyClass(CompositeKey.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(pathtemp));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }    
}
