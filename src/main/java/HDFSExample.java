import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;


public class HDFSExample {


    public static void main(String[] args)throws Exception{


        Configuration conf=new Configuration();

        FileSystem fs=
        FileSystem.get(
        URI.create("hdfs://localhost:9000"),
        conf);


        Path file=new Path("/user/hadoop/test/test.txt");


        //创建文件

        FSDataOutputStream out=
        fs.create(file);

        out.write(
        "Hello Hadoop".getBytes()
        );

        out.close();



        //读取文件

        FSDataInputStream in=
        fs.open(file);

        System.out.println(
        in.readLine()
        );

        in.close();



        //文件信息

        FileStatus status=
        fs.getFileStatus(file);


        System.out.println(
        "size:"
        +status.getLen()
        );



        //删除

        fs.delete(file,false);



        fs.close();

    }

}
