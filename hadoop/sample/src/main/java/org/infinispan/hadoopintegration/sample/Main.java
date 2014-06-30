package org.infinispan.hadoopintegration.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputFormat;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputFormat;

import java.io.IOException;

public class Main {

   public static void main(String[] args) throws IOException {
      Configuration configuration = new Configuration();
      if (args.length != 2) {
         System.err.println("Usage: hadoop jar <job jar> Main <ispn-server>");
         System.exit(2);
      }
      String host = args[1];
      configuration.set("mapreduce.ispn.inputsplit.remote.cache.host", host);

        configuration.set("mapreduce.ispn.input.remote.cache.host", host);
        configuration.set("mapreduce.ispn.output.remote.cache.host", host);

        configuration.set("mapreduce.ispn.input.cache.name", "map-reduce-in");
        configuration.set("mapreduce.ispn.output.cache.name", "map-reduce-out");

        configuration.set("mapreduce.ispn.input.converter", InputConverter.class.getCanonicalName());
        configuration.set("mapreduce.ispn.output.converter", OutputConverter.class.getCanonicalName());

        JobConf jobConf = new JobConf(configuration, Main.class);
        jobConf.setJobName("wordcount");

        /*jobConf.setOutputKeyClass(String.class);
        jobConf.setOutputValueClass(Integer.class);
        jobConf.setOutputKeyComparatorClass(StringComparator.class);
        jobConf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization,org.apache.hadoop.io.serializer.JavaSerialization");
        */

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(MapClass.class);
        jobConf.setReducerClass(ReduceClass.class);

        //FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        //FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
        jobConf.setInputFormat(InfinispanInputFormat.class);
        jobConf.setOutputFormat(InfinispanOutputFormat.class);
        System.out.println("About to run the job!!!!");
        JobClient.runJob(jobConf);

        System.out.println("Finished executing job.");
        System.out.println("CACHE CONTENT:");

        /*RemoteCacheManager remoteCacheManager = new RemoteCacheManager("10.35.23.11");
        System.out.println(" =========================== INPUT ================================ ");
        //System.out.println(remoteCacheManager.getCache("map-reduce-in").getBulk());
        System.out.println(" ================================================================== ");
        System.out.println(" =========================== OUTPUT =============================== ");
        //System.out.println(remoteCacheManager.getCache("map-reduce-out").getBulk());
        System.out.println(" ================================================================== ");
        */
    }

    public static class StringComparator implements RawComparator<String> {

        @Override
        public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3, int i4) {
            return WritableComparator.compareBytes(bytes, i, i2, bytes2, i3, i4);
        }

        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }
}
