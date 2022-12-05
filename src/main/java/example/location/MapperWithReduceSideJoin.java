package example.location;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperWithReduceSideJoin extends Mapper<LongWritable, Text, TaggedKey, Text> {

    TaggedKey outputKey = new TaggedKey();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        BusStationDTO dto = new BusStationDTO(value);
        outputKey.setstationCode(dto.getStationID());
        outputKey.setTag(1);
        context.write(outputKey, value);
    }
}
