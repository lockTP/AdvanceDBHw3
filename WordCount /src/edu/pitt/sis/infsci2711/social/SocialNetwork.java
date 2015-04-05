package edu.pitt.sis.infsci2711.social;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SocialNetwork extends Configured implements Tool {
	
   public static HashMap<Integer, ArrayList<Integer>> friendsList = new HashMap<Integer, ArrayList<Integer>>();
   public static int inputID;
	
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new SocialNetwork(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      inputID = Integer.parseInt(args[2]);
      BufferedReader br = new BufferedReader(new FileReader(args[0]));
      try {
          String line = br.readLine();

          while (line != null) {
        	  
        	  String[] temp = line.toString().split("	");
		        	  if(temp.length > 1){
		        		  String[] friends = temp[1].toString().split(",");
		        		  ArrayList<Integer> f = new ArrayList<Integer>();
		    	    	  for (int i = 0; i < friends.length; i++) {
		    	    		  f.add(Integer.parseInt(friends[i]));
		    	    	  }
		    	    	  friendsList.put(Integer.parseInt(temp[0]), f);
		        	  }else{
		        		  friendsList.put(Integer.parseInt(temp[0]), null);
		        	  }
              line = br.readLine();
              
          }
      } finally {
          br.close();
      }
      
      Job job = new Job(getConf(), "SocialNetwork");
      job.setJarByClass(SocialNetwork.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();
      private int v1;
      private int v2;
      private int tempF;
      private String relation;
      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  String[] temp = value.toString().split("	");
    	  if(temp.length > 1){
    		  String[] friends = temp[1].toString().split(",");
    		  if(Integer.parseInt(temp[0]) == inputID){
		    	  for (int i = 0; i < friends.length; i++) {
		    		 v1 = Integer.parseInt(temp[0]);
		    		 tempF = Integer.parseInt(friends[i]);
		    		 ArrayList<Integer> f = SocialNetwork.friendsList.get(tempF);
		    		 for(int j = 0; j < f.size(); j++){
		    			 v2 = f.get(j);
		    			 ArrayList<Integer> cur = SocialNetwork.friendsList.get(v1);
		    			 boolean flag = true;
		    			 for(int k = 0; k < cur.size(); k++){
		    				 if (v2 == cur.get(k) || v2 == v1) {
		    					 flag = false;
		    					
		    				 }
		    			 }
		    			 if(flag == true){
			    			 if(v1 < v2){
				    			 relation = v1 + "_" + v2;
				    		 }else{
				    			 relation = v2 + "_" + v1;
				    		 }
				    		 word.set(relation);
				             context.write(word, ONE);
		    			 }
		    		 }
		    	  }
    		  }
    	  }
      }
   }

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         context.write(key, new IntWritable(sum));
      }
   }
   
  
}