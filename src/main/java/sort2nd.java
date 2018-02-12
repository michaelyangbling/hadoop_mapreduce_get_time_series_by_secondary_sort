import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Partitioner;

/*Pseudo code
class mapper {
   map(line, text){
   emit( [ station, year ], [ (min temp, 1 or 0), (max temp, 1 or 0) ,year ] )
   }
class partitioner{
   partition by station}

class groupComparator{
   compare only by station
}

class reducer{
   reduce( [station, year ] [ ..., [ (min temp, 1or0), (max temp, 1or0),year ], ...] )
     emit( station, [year1, mean min temp, min max temp],
      [year2, mean min temp, min max temp],
      ... )
key comparator is by station and year ( defined by compareTo in staYear )
*/


class staYear implements WritableComparable<staYear> {
     String station;
     int year;
    public staYear(){}
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, station);
        out.writeInt(year);
    }
    public void readFields(DataInput in) throws IOException{
        station = WritableUtils.readString(in);
        year=in.readInt();
    }

    public int compareTo(staYear sy) {
        int cmp = station.compareTo(sy.station);
        if (cmp != 0) {
            return cmp; }
        return sort2nd.compare(year,sy.year); }


}//define a cutom key (station, year)

class info implements Writable {
    int Count_max; float Sum_max;
    int Count_min; float Sum_min;
    int year;
    public info() {

    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(Count_max);out.writeFloat(Sum_max);
        out.writeInt(Count_min);out.writeFloat(Sum_min);
        out.writeInt(year);
    }
    public void readFields(DataInput in) throws IOException {
        Count_max = in.readInt();Sum_max = in.readFloat();
        Count_min = in.readInt();Sum_min = in.readFloat();
        year=in.readInt();

    }
}//define a custom value( Count_max, Sum_max, Count_min, Sum_min, year )
public class sort2nd {

    public static int compare(int a,int b){
        if(a<b){ return -1;}
        else if(a==b) {return 0;}
        else{return 1;}
    }
    public static class groupComparator extends WritableComparator {
        protected groupComparator(){
            super(staYear.class, true);
        }
        public int compare(WritableComparable w1, WritableComparable w2){
            staYear sy1=(staYear) w1;
            staYear sy2=(staYear) w2;
            return sy1.station.compareTo(sy2.station);
        }
    }// group comparator only compare station
    public static class myMapper
            extends Mapper<Object, Text, staYear, info>{
        private  staYear sy = new staYear();
        private  info pair = new info();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            List<String> line2=new ArrayList<String>(Arrays.asList(line.split(",")));
            pair.Count_max=0;pair.Sum_max=0;pair.Count_min=0;pair.Sum_min=0;//reset for method
            if( line2.get(2).equals("TMAX") ) {
                int year=Integer.parseInt(line2.get(1).substring(0,4));
                sy.station=line2.get(0); sy.year=year;
                pair.Count_max+=1; pair.Sum_max+=Float.parseFloat(line2.get(3));pair.year=year;
                context.write(sy,pair);
            }
            else if (line2.get(2).equals("TMIN") ) {
                int year=Integer.parseInt(line2.get(1).substring(0,4));
                sy.station=line2.get(0); sy.year=year;
                pair.Count_min+=1;pair.Sum_min+=Float.parseFloat(line2.get(3));pair.year=year;
                context.write(sy,pair);}
            else {}

        }

    }

    public static class myPartitioner
            extends Partitioner<staYear, info> { //partition(hash) by station(string)
        public int getPartition(staYear key, info value, int numPartitions) {
            return Math.abs(key.station.hashCode()) % numPartitions;
        }
    }

    public static class myReducer
            extends Reducer<staYear,info,Text,Text> {
        //a reduce function will call ( [station, year ] [ ..., [ (min temp, 1or0), (max temp, 1or0) , year], ...] ) for every station,
        // since groupComparator  compare only by station, not considering year.

        //Also, since key comparator is by station and year,
        // in each reduce call values will be in the year order in the input list.
        public void reduce(staYear key, Iterable<info> iterable,
                           Context context
        ) throws IOException, InterruptedException {// seems hadoop is strange with multiple iterators
            int count_max=0;int count_min = 0;
            float sum_max=0;float sum_min=0;
            List<info> values=new ArrayList<info>();
            info copy;
            for(info val : iterable){//hadoop use same object for Iterable, so must make copy
                copy=new info();
                copy.year=val.year;
                copy.Count_max=val.Count_max;copy.Count_min=val.Count_min;
                copy.Sum_max=val.Sum_max;copy.Sum_min=val.Sum_min;
                values.add(copy);
            }
            String maxString, minString; float maxMean,minMean;
            String all=""; //to contain whole-station information
            int i=0; info k;
             while (i<=values.size()-1) {
                k=values.get(i);
                if(i<values.size()-1) {//if not the last element
                  if(k.year!=values.get(i+1).year){ //if a different year appears
                      if (count_max==0){maxString="this station has no tmax record";}
                      else{ maxMean=sum_max/count_max; maxString=Float.toString(maxMean);}
                      if (count_min==0){minString="this station has no tmin record";}
                      else{ minMean=sum_min/count_min; minString=Float.toString(minMean);}
                      all=all+"year"+Integer.toString(k.year)+", "+minString+", "+maxString+"  ";
                      count_max=0; count_min = 0;sum_max=0; sum_min=0;
                  }
                  else{
                      count_max+=k.Count_max; sum_max+=k.Sum_max;
                      count_min+=k.Count_min; sum_min+=k.Sum_min;
                  }
                }
                else{ //the last element
                    count_max+=k.Count_max; sum_max+=k.Sum_max;
                    count_min+=k.Count_min; sum_min+=k.Sum_min;
                    if (count_max==0){maxString="this station has no tmax record";}
                    else{ maxMean=sum_max/count_max; maxString=Float.toString(maxMean);}
                    if (count_min==0){minString="this station has no tmin record";}
                    else{ minMean=sum_min/count_min; minString=Float.toString(minMean);}
                    all=all+"year"+Integer.toString(k.year)+", "+minString+", "+maxString+"  ";
                }
                i=i+1;
            }
            context.write(new Text(key.station) , new Text(all));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(sort2nd.class);

        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReducer.class);
        job.setPartitionerClass(myPartitioner.class);
        job.setGroupingComparatorClass(groupComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(staYear.class);
        job.setMapOutputValueClass(info.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




}




