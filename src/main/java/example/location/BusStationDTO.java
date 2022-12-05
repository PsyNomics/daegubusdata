package example.location;

import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class BusStationDTO {
    private String stationID;

    public BusStationDTO(Text text) {
        String[] columns = text.toString().split(",");
        stationID = columns[0];

    }

    public String getStationID() {
        return stationID;
    }
}
