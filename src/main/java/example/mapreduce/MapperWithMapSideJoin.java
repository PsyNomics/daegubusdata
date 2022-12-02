package example.mapreduce;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Hashtable;

public class MapperWithMapSideJoin extends Mapper<LongWritable, Text, Text, Text> {
    private Hashtable<String, String> joinMap = new Hashtable<String, String>();
    private Text outputKey = new Text();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = DistributedCache.getLo
    }

}
