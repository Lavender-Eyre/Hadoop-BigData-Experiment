import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedIndex {


    public static class IndexMapper
            extends Mapper<Object, Text, Text, Text>{


        private Text word = new Text();
        private Text document = new Text();


        HashSet<String> stopWords = new HashSet<>();


        protected void setup(Context context){

            String[] words={
                "a","an","the","is","are",
                "of","and","or","to",
                "in","on","for","with",
                "from","this","that"
            };


            for(String w:words){
                stopWords.add(w);
            }

        }



        public void map(Object key,
                        Text value,
                        Context context)

                throws IOException,InterruptedException{


            String filename =
            ((org.apache.hadoop.mapreduce.lib.input.FileSplit)
            context.getInputSplit())
            .getPath()
            .getName();



            StringTokenizer tokenizer =
            new StringTokenizer(
            value.toString());


            while(tokenizer.hasMoreTokens()){


                String w =
                tokenizer.nextToken()
                .toLowerCase()
                .replaceAll("[^a-z]","");



                if(w.length()>0
                    && !stopWords.contains(w)){


                    word.set(w);

                    document.set(filename);


                    context.write(word,document);

                }

            }


        }

    }




    public static class IndexReducer
            extends Reducer<Text,Text,Text,Text>{



        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)

                throws IOException,InterruptedException{


            String result="";

            int total=0;



            for(Text value:values){


                result += "<"
                        +value.toString()
                        +">;";

                total++;

            }


            result += "<total,"
                    +total
                    +">";


            context.write(
                key,
                new Text(result));

        }


    }





    public static void main(String[] args)
            throws Exception{


        Configuration conf =
                new Configuration();


        Job job =
        Job.getInstance(
                conf,
                "inverted index");



        job.setJarByClass(
                InvertedIndex.class);



        job.setMapperClass(
                IndexMapper.class);



        job.setReducerClass(
                IndexReducer.class);



        job.setOutputKeyClass(
                Text.class);


        job.setOutputValueClass(
                Text.class);



        FileInputFormat.addInputPath(
                job,
                new Path(args[0]));



        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1]));



        System.exit(
        job.waitForCompletion(true)
        ?0:1);


    }

}
