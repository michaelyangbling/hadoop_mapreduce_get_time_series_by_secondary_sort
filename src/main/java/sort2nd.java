import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import java.io.*;

class staYear implements WritableComparable<staYear>{
    Text station;
    int year;
    public staYear(){}
    public void write(DataOutput out) throws IOException{
        station.write(out);
        out.writeInt(year);
    }
    public void readFields(DataInput in) throws IOException{
        station.readFields(in);
        year=in.readInt();
    }

    public int compareTo(staYear sy) {
        int cmp = station.compareTo(sy.station);
        if (cmp != 0) {
            return cmp; }
        return sort2nd.compare(year,sy.year); }


}//define a cutom key (station, year)

public class sort2nd {

    public static int compare(int a,int b){
        if(a<b){ return -1;}
        else if(a==b) {return 0;}
        else{return 1;}
    }
    public static class groupComparator extends WritableComparable {
        protected groupComparator(){
            super(staYear.class, true);
        }
        public int compare(WritableComparable w1, WritableComparable w2){
            staYear sy1=(staYear) w1;
            staYear sy2=(staYear) w2;
            return sy1.station.compareTo(sy2.station);
        }
    }// group comparator only compare station

}



