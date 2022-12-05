package example.location;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements WritableComparable<TaggedKey> {
    // 항공사 코드
    private String stationCode;
    // 조인 테그
    private Integer tag;

    public TaggedKey() {}

    public TaggedKey(String stationCode, int tag) {
        this.stationCode = stationCode;
        this.tag = tag;
    }

    public String getstationCode() {
        return stationCode;
    }

    public Integer getTag() {
        return tag;
    }

    public void setstationCode(String stationCode) {
        this.stationCode = stationCode;
    }

    public void setTag(Integer tag) {
        this.tag = tag;
    }

    @Override
    public int compareTo(TaggedKey key) {
        int result = this.stationCode.compareTo(key.stationCode);

        if (result == 0) {
            return  this.tag.compareTo(key.tag);
        }

        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, stationCode);
        out.writeInt(tag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationCode = WritableUtils.readString(in);
        tag = in.readInt();
    }
}