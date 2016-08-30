
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.*;
import java.text.*;

public class PlayerCount 
{

  public static class ReMapper extends Mapper<Object, Text, Text, Text>
  {
    private Text win = new Text("1");
    private Text lose = new Text("-1"); 
    private Text draw = new Text("0");
    Text player_white= new Text();
    Text player_black = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {   
        try
        {       
            String[] strs= value.toString().split("\n");
            for(int i=0;i<strs.length;i++)
            {                
                String str = strs[i];
                if(str.contains("White "))
                {
                    String name = str.trim().replace("White ", "");
                    name=name.replaceAll("\"", "");
                    name=name.replaceAll("\\[", "");
                    name=name.replaceAll("\\]", "");
                    player_white.set(name+" white");
                }
                else if(str.contains("Black "))
                {
                    String name = str.trim().replace("Black ", "");
                    name=name.replaceAll("\"", "");
                    name=name.replaceAll("\\[", "");
                    name=name.replaceAll("\\]", "");        
                    player_black.set(name+" Black");
                }
                
                if(str.contains("Result"))
                {
                    if(str.contains("0-1"))
                    {
                        context.write(player_white, lose);                      
                        context.write(player_black, win);
                    }
                    else if(str.contains("1-0"))
                    {
                        context.write(player_white, win);
                        context.write(player_black, lose);
                    }
                    else if(str.contains("1/2-1/2"))
                    {
                        context.write(player_white, draw);
                        context.write(player_black, draw);
                    }               
                }
            }
        }
        catch(Exception e)
        {e.printStackTrace();}
    }
  }

  public static class Combiner extends Reducer<Text,Text,Text,Text> 
  {
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
    {   
        int win_num=0;
        int lose_num=0;
        int draw_num=0; 
        for(Text d:values)
        {
            if(d.toString().equals("1"))
                win_num+=1;
            else if(d.toString().equals("-1"))
                lose_num+=1;
            else if(d.toString().equals("0"))
                draw_num+=1;
        }
        String result = win_num+"\t"+lose_num+"\t"+draw_num;
        Text res = new Text(result);
        context.write(key,res);
    }
}

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> 
  {
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
    {   
        double win_num=0;
        double lose_num=0;
        double draw_num=0;  
        for(Text d:values)
        {
            String[] score = d.toString().split("\t");
            win_num = Double.parseDouble(score[0]);
            lose_num = Double.parseDouble(score[1]);
            draw_num = Double.parseDouble(score[2]);
        }
        double sum=win_num+lose_num+draw_num;
        win_num = win_num/sum;
        lose_num=lose_num/sum;
        draw_num=draw_num/sum;
        win_num = Math.round(win_num*100.0)/100.0;
        lose_num = Math.round(lose_num*100.0)/100.0;
        draw_num = Math.round(draw_num*100.0)/100.0;

        String score = win_num+"\t"+lose_num+"\t"+draw_num;
        Text t = new Text(score);
        context.write(key,t);
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    conf.set("textinputformat.record.delimiter","}");
    Job job = Job.getInstance(conf, "Player count");
    job.setJarByClass(PlayerCount.class);
    job.setMapperClass(ReMapper.class);
    job.setCombinerClass(Combiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}