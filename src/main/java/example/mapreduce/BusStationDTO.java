package example.mapreduce;

import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class BusStationDTO {
    private String stationID;
    private String dayOfWeek;
    private String week;
    private int passengerNum = 0;

    public BusStationDTO(Text text) {
        try {
            String[] columns = text.toString().split(",");
            stationID = columns[2];
            passengerNum = Integer.parseInt(columns[25]);

            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            String sdf1Date = columns[0];
            Date date = new Date(sdf1.parse(sdf1Date).getTime());
            
            SimpleDateFormat sdf2 = new SimpleDateFormat("E", Locale.KOREA);
            dayOfWeek = sdf2.format(date);

            switch (dayOfWeek) {
                case "월":
                case "화":
                case "수":
                case "목":
                case "금":
                    week = "주중(월~금)";
                    break;
                case "토":
                case "일":
                    week = "주말(토~일)";
                    break;
            }
            
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public String getStationID() {
        return stationID;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public String getWeek() {
        return week;
    }

    public int getPassengerNum() {
        return passengerNum;
    }
}
