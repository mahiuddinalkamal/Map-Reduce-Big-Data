package solutions.assignment2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;

public class MapRedSolution2
{
	
	public static class MapRecords extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable ONE = new IntWritable(1);

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{ 
			try{
				String line = value.toString();
				String[] taxiData = line.split(",");
				int hour  = Integer.parseInt(taxiData[1].substring(11,13));
				int min = Integer.parseInt(taxiData[1].substring(14,16));

				if ((hour>=0 && hour<1) && (min>=0 && min <=59)) context.write(new Text("12am"), ONE);
				
				else if ((hour>=1 && hour<2) && (min>=0 && min <=59)) context.write(new Text("1am"), ONE);		
			
				else if ((hour>=2 && hour<3) && (min>=0 && min <=59)) context.write(new Text("2am"), ONE);		
				
				else if ((hour>=3 && hour<4) && (min>=0 && min <=59)) context.write(new Text("3am"), ONE);

				else if ((hour>=4 && hour<5) && (min>=0 && min <=59)) context.write(new Text("4am"), ONE);
				
				else if ((hour>=5 && hour<6) && (min>=0 && min <=59)) context.write(new Text("5am"), ONE);		
			
				else if ((hour>=6 && hour<7) && (min>=0 && min <=59)) context.write(new Text("6am"), ONE);		
				
				else if ((hour>=7 && hour<8) && (min>=0 && min <=59)) context.write(new Text("7am"), ONE);

				else if ((hour>=8 && hour<9) && (min>=0 && min <=59)) context.write(new Text("8am"), ONE);		
			
				else if ((hour>=9 && hour<10) && (min>=0 && min <=59)) context.write(new Text("9am"), ONE);		
				
				else if ((hour>=10 && hour<11) && (min>=0 && min <=59)) context.write(new Text("10am"), ONE);

				else if ((hour>=11 && hour<12) && (min>=0 && min <=59)) context.write(new Text("11am"), ONE);
				
				else if ((hour>=12 && hour<13) && (min>=0 && min <=59)) context.write(new Text("12pm"), ONE);		
			
				else if ((hour>=13 && hour<14) && (min>=0 && min <=59)) context.write(new Text("1pm"), ONE);		
				
				else if ((hour>=14 && hour<15) && (min>=0 && min <=59)) context.write(new Text("2pm"), ONE);	

				else if ((hour>=15 && hour<16) && (min>=0 && min <=59)) context.write(new Text("3pm"), ONE);		
			
				else if ((hour>=16 && hour<17) && (min>=0 && min <=59)) context.write(new Text("4pm"), ONE);		
				
				else if ((hour>=17 && hour<18) && (min>=0 && min <=59)) context.write(new Text("5pm"), ONE);		
				
				else if ((hour>=18 && hour<19) && (min>=0 && min <=59)) context.write(new Text("6pm"), ONE);		
			
				else if ((hour>=19 && hour<20) && (min>=0 && min <=59)) context.write(new Text("7pm"), ONE);		
				
				else if ((hour>=20 && hour<21) && (min>=0 && min <=59)) context.write(new Text("8pm"), ONE);		

				else if ((hour>=21 && hour<22) && (min>=0 && min <=59)) context.write(new Text("9pm"), ONE);		
			
				else if ((hour>=22 && hour<23) && (min>=0 && min <=59)) context.write(new Text("10pm"), ONE);		
				
				else if ((hour>=23 && hour<24) && (min>=0 && min <=59)) context.write(new Text("11pm"), ONE);
			}
			catch(Exception e) { }
		}
	 }
	
    public static class ReduceRecords extends Reducer<Text, IntWritable, Text, IntWritable>
    {
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
            {
                int sum = 0;
            
                for (IntWritable val : values)
                   sum += val.get();
                
                context.write(key, new IntWritable(sum));
            }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #2");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(MapRecords.class);
	job.setCombinerClass(ReduceRecords.class);
        job.setReducerClass(ReduceRecords.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1; 

        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1]+"/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();
        
        String[] validMd5Sums = {"03357cb042c12da46dd5f0217509adc8", "ad6697014eba5670f6fc79fbac73cf83", "07f6514a2f48cff8e12fdbc533bc0fe5", 
            "e3c247d186e3f7d7ba5bab626a8474d7", "fce860313d4924130b626806fa9a3826", "cc56d08d719a1401ad2731898c6b82dd", 
            "6cd1ad65c5fd8e54ed83ea59320731e9", "59737bd718c9f38be5354304f5a36466", "7d35ce45afd621e46840627a79f87dac"};
        
        for (String validMd5 : validMd5Sums) 
        {
            if (validMd5.contentEquals(md5))
            {
                System.out.println("The result looks good :-)");
                System.exit(exitCode);
            }
        }
        System.out.println("The result does not look like what we expected :-(");
        System.exit(exitCode);
    }
}

