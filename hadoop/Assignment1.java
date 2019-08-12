package assignment1;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Assignment1 {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, myMapWritable> {
        //create a mapwritable to save counter and filename
        myMapWritable tags_map = new myMapWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //get first argument from system input
            Configuration conf = context. getConfiguration();

            String num_ngram1 = conf.get("num_ngram");

            int num_ngram = Integer.parseInt(num_ngram1);
            //get the filename
            String inputname = ((FileSplit) context.getInputSplit()).getPath().getName();
            Text id = new Text(inputname);
            String line = value.toString();
            line = line.trim().toLowerCase();
            String[] words = line.split("\\s+");
            StringBuilder sb;
            //get every ngram from each file
            for(int i = 0; i < words.length-1; i++){

                sb = new StringBuilder();
                sb.append(words[i]);
                for(int j=1; i+j<words.length && j<num_ngram; j++){
                    sb.append(" ");
                    sb.append(words[i+j]);
                    if(j==num_ngram-1) {
                        tags_map.put(id, new IntWritable(1));
                        context.write(new Text(sb.toString().trim()), tags_map);
                    }
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, myMapWritable, Text, myMapWritable> {



        public void reduce(Text key, Iterable<myMapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //create a mapwritable to save result
            myMapWritable result = new myMapWritable();
            //get second argument
            Configuration conf = context. getConfiguration();
            String min_count1 = conf.get("min_count");
            int sum = 0;
            String s = "";
            int cnt = 0;
            String id = "";
            int min_count = Integer.parseInt(min_count1);
            for (myMapWritable val : values) {
                for(Writable ele : val.keySet()){
                    cnt = ((IntWritable)val.get(ele)).get();
                    id = ((Text)ele).toString();
                }
                sum += cnt;
                s = s+id+" ";
            }
            //set the threshold
            if(sum >= min_count) {
                result.put(new IntWritable(sum),new Text(s));
                context.write(key, result);
            }

        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("num_ngram",args[0]);
        conf.set("min_count",args[1]);
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Assignment1.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(myMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


//make the output correct
class myMapWritable extends MapWritable{

    @Override
    public String toString(){
        String s = new String("");

        Set<Writable> keys = this.keySet();
        for (Writable key : keys) {
            Text count = (Text) this.get(key);
            s = s + key.toString() + "  " + count.toString();
        }

        return s;
    }
}
