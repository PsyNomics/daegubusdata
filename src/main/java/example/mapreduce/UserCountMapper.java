package example.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0)
            return;

        BusStationDTO dto = new BusStationDTO(value);

//        outputKey.set("dayOfWeek," + dto.getDayOfWeek() + "," + dto.getStationID());
//        outputValue.set(dto.getPassengerNum());
//        context.write(outputKey, outputValue);

        outputKey.set("week," + dto.getStationID() + "," + dto.getWeek());
        outputValue.set(dto.getPassengerNum());
        context.write(outputKey, outputValue);
    }
}
