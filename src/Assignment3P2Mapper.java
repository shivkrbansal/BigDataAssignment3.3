



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Assignment3P2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
    	if(value.toString().split("|")!=null){

            String[] lineArray = value.toString().split("\\|");
            
            String company=lineArray[0];
            if(lineArray.length>4){
              IntWritable Val = new IntWritable(1);
              
              

     if((company.equals("NA")) ||(lineArray[1].equals("NA"))){
    	 
               }
    	
        else{
        	if(lineArray[0].equals("Onida"))
    			
			{
	         Text productName = new Text(lineArray[1]);
             Text State =new Text(lineArray[3]);
             context.write(State,Val);
        	}
            
        }
        }}}}
                  
                
        
    