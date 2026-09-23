import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Family {


/*
Mapper:

输入:
child parent

输出:
parent child

例如:

Steven Lucy

变成:

Lucy Steven

*/

public static class FamilyMapper
extends Mapper<Object,Text,Text,Text>{


public void map(
Object key,
Text value,
Context context)
throws IOException,InterruptedException{


String[] data =
value.toString().split("\\s+");


if(data.length==2){


String child=data[0];

String parent=data[1];


// parent作为key

context.write(
new Text(parent),
new Text(child)
);


}


}

}





/*
Reducer:

同一个parent下面：

parent:

Lucy

children:

Steven
Jone

父母:

Mary
Frank


生成：

Steven Mary
Steven Frank

*/

public static class FamilyReducer
extends Reducer<Text,Text,Text,Text>{



public void reduce(
Text key,
Iterable<Text> values,
Context context)
throws IOException,InterruptedException{


ArrayList<String> list =
new ArrayList<>();


for(Text value:values){

list.add(value.toString());

}



for(int i=0;i<list.size();i++){


for(int j=0;j<list.size();j++){


if(i!=j){


context.write(
new Text(list.get(i)),
new Text(list.get(j))
);


}


}


}


}

}





public static void main(String[] args)
throws Exception{


Configuration conf=
new Configuration();


Job job=
Job.getInstance(conf,"family");



job.setJarByClass(Family.class);



job.setMapperClass(FamilyMapper.class);

job.setReducerClass(FamilyReducer.class);



job.setMapOutputKeyClass(Text.class);

job.setMapOutputValueClass(Text.class);



job.setOutputKeyClass(Text.class);

job.setOutputValueClass(Text.class);



FileInputFormat.addInputPath(
job,
new Path(args[0])
);



FileOutputFormat.setOutputPath(
job,
new Path(args[1])
);



System.exit(
job.waitForCompletion(true)
?0:1
);



}


}
