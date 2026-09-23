import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Sort {


    public static class SortMapper
    extends Mapper<Object, Text, IntWritable, IntWritable>{


        public void map(
        Object key,
        Text value,
        Context context)
        throws IOException,InterruptedException{


            int number =
            Integer.parseInt(value.toString());


            context.write(
            new IntWritable(number),
            new IntWritable(1)
            );

        }

    }



    public static class SortReducer
    extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{


        int index = 1;


        public void reduce(
        IntWritable key,
        Iterable<IntWritable> values,
        Context context)
        throws IOException,InterruptedException{


            context.write(
            new IntWritable(index),
            key
            );


            index++;

        }

    }



    public static void main(String[] args)
    throws Exception{


        Configuration conf =
        new Configuration();


        Job job =
        Job.getInstance(conf,"sort");


        job.setJarByClass(Sort.class);


        job.setMapperClass(SortMapper.class);

        job.setReducerClass(SortReducer.class);



        job.setMapOutputKeyClass(IntWritable.class);

        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(IntWritable.class);

        job.setOutputValueClass(IntWritable.class);



        FileInputFormat.addInputPath(
        job,
        new Path(args[0])
        );


        FileOutputFormat.setOutputPath(
        job,
        new Path(args[1])
        );


        System.exit(
        job.waitForCompletion(true)?0:1
        );

    }

}
