package example.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopUserMapper extends Mapper<Object, Text, Text, LongWritable> {
    private TreeMap<Long, String> tmap;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        tmap = new TreeMap<Long, String>();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        String movie_name = tokens[0];

        long no_of_views = Long.parseLong(tokens[1]);
        tmap.put(no_of_views, movie_name);
        if (tmap.size() > 50) {
            tmap.remove(tmap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : tmap.entrySet()) {
            long count = entry.getKey();
            String name = entry.getValue();
            context.write(new Text(name), new LongWritable(count));
        }
    }
}
