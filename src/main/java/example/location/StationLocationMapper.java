package example.location;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StationLocationMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
    TaggedKey outputKey = new TaggedKey();
    Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StationLocationDTO dto = new StationLocationDTO(value);

        outputKey.setstationCode(dto.getStationCode());
        outputKey.setTag(0);
        outValue.set(dto.getLongitude() + "," + dto.getLatitude());

        context.write(outputKey, outValue);
    }
}
