/**
 * @author HaoWang
 *
 */
package CommonFriends;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CommonFriends {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    
    private Text intermediate_key = new Text();
    private Text intermediate_value = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
        StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
        String line="";
        String []elements;
        
        
        while (itr.hasMoreTokens()) {
          line=itr.nextToken();
          elements=line.split(" ");
          String self=elements[0];
          line=" ";
          	for(int i=1;i<elements.length;i++){
          		
          		line+=elements[i]+" ";
          	}
          	for(int i=1;i<elements.length;i++){
          		String anyone=elements[i];
          		if(self.compareTo(anyone)>0){        			
          			intermediate_key.set(anyone+","+self);       			
          			
          		}
          		else
          		{
          			intermediate_key.set(self+","+anyone);        			
          			
          		}
          		
          		intermediate_value.set(line);
          		context.write(intermediate_key,intermediate_value);
          	}	
          	
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result=new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
     List<String> listOfFriends=new ArrayList<String>();
     List<String> commonFriendList=new ArrayList<String>();
     boolean keepWhile;
     String strA,strB;
     String commonFriend="";
     String[]elements;
     //String sum="";
    	for(Text vl:values)
    	{
    		elements=vl.toString().split(" ");
    		for(String el:elements){
    			listOfFriends.add(el);
    			//sum+=el+" ";//test
    		}
    		
    	}
    
    	for(int i=0;i<listOfFriends.size()-1;i++)
    	{
    		keepWhile=true;
    		strA=listOfFriends.get(i);
    		
    		if(!commonFriendList.isEmpty()&&commonFriendList.contains(strA)){  			
    				keepWhile=false;
    			
    		}
    		int j=i+1;
    		while(keepWhile && j<listOfFriends.size()){
    			
    			
    			
    			strB=listOfFriends.get(j);
	    		if(strA.equals(strB))
	    		{
	    				
	    			commonFriendList.add(strA);	    				
	    			keepWhile=false;
	    		}
    			j++;
    			  	
    			
    			
    		}
    			
    		
    	}
    	for(int i=0;i<commonFriendList.size();i++){
    		
    		if(i==commonFriendList.size()-1){
    			commonFriend+=commonFriendList.get(i);
    		}
    		else{
    			commonFriend=commonFriend+commonFriendList.get(i)+",";
    		}
    		
    		
    	}
    	if(commonFriend.length()>2){
    		
    		commonFriend=commonFriend.substring(1);
    	}
    

    	result.set(commonFriend);
    	//test
    //	result.set(sum);
    	context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(CommonFriends.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}