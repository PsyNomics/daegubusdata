package example.location;

import org.apache.hadoop.io.Text;

public class StationLocationDTO {
    private String stationCode;
    private String longitude;
    private String latitude;

    public StationLocationDTO(Text value) {
        this(value.toString());
    }

    public StationLocationDTO(String value) {
        try {
            String[] colums = value.split(",");
            if (colums != null && colums.length > 0) {
                stationCode = colums[0];
                longitude = colums[7];
                latitude = colums[8];
            }
        } catch (Exception e) {
            System.out.println("Error parsing a record :" + e.getMessage());
        }
    }

    public String getStationCode() {
        return stationCode;
    }

    public String getLongitude() {
        return longitude;
    }

    public String getLatitude() {
        return latitude;
    }
}
