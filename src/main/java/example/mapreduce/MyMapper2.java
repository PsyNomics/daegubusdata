package example.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper2 extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map (Text key, Text value, final Context context) throws IOException, InterruptedException {
        context.write(value, new Text(key + "\t" + 2));
    }
}
