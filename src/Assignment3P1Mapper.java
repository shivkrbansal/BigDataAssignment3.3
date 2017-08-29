

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Assignment3P1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

       if(value.toString().split("|")!=null){

        String[] lineArray = value.toString().split("\\|");
        if(lineArray.length>4){
        String company=lineArray[0];
        
          IntWritable Val = new IntWritable(1);
          String product = lineArray[1];
          

 if((company.equals("NA")) ||(product.equals("NA"))){
	 
           }
        else{
            context.write(new Text(company),Val);
        }
    }}}}


