/**
 * Created by k5zhang on 2015/9/7.
 */

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple10;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
* hbase content:
* hbase(main):003:0> scan 'scores'
ROW                             COLUMN+CELL
 Jim                            column=course:art, timestamp=1441549968185, value=80
 Jim                            column=course:math, timestamp=1441549968167, value=89
 Jim                            column=grade:, timestamp=1441549968142, value=4
 Tom                            column=course:art, timestamp=1441549912222, value=88
 Tom                            column=course:math, timestamp=1441549842348, value=97
 Tom                            column=grade:, timestamp=1441549820516, value=5
2 row(s) in 0.1330 seconds
*/
public class spark_hbase_main {
    private static String appName = "Hello";
    private static String startTime = "201409_0930";
    private static String stopTime = "2014";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        Configuration conf = HBaseConfiguration.create();

        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes(startTime));
        //scan.setStopRow(Bytes.toBytes(stopTime));
        scan.addFamily(Bytes.toBytes("course"));
        scan.addColumn(Bytes.toBytes("course"), Bytes.toBytes("art"));
        scan.addColumn(Bytes.toBytes("course"), Bytes.toBytes("math"));

        String scanToString = "";
        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            scanToString = Base64.encodeBytes(proto.toByteArray());
        } catch (IOException io) {
            System.out.println(io);
        }

        for (int i = 0; i < 2; i++) {
            try {
                String tableName = "scores";
                conf.set(TableInputFormat.INPUT_TABLE, tableName);
                conf.set(TableInputFormat.SCAN, scanToString);

                //获得hbase查询结果Result
                JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf,
                        TableInputFormat.class, ImmutableBytesWritable.class,
                        Result.class);

                JavaPairRDD<String, List<Integer>> art_scores = hBaseRDD.mapToPair(
                        new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, List<Integer>>() {
                            @Override
                            public Tuple2<String, List<Integer>> call(Tuple2<ImmutableBytesWritable, Result> results) {

                                List<Integer> list = new ArrayList<Integer>();

                                byte[] art_score = results._2().getValue(Bytes.toBytes("course"), Bytes.toBytes("art"));
                                byte[] math_score = results._2().getValue(Bytes.toBytes("course"), Bytes.toBytes("math"));

                                list.add(Integer.parseInt(Bytes.toString(art_score)));
                                list.add(Integer.parseInt(Bytes.toString(math_score)));

                                return new Tuple2<String, List<Integer>>(Bytes.toString(results._1().get()), list);
                            }
                        }
                );
                //System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxx" + art_scores.collect());

                JavaPairRDD<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>> cart = art_scores.cartesian(art_scores);

                JavaPairRDD<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>> cart2 = cart.filter(
                        new Function<Tuple2<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>>, Boolean>() {
                            public Boolean call(Tuple2<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>> tuple2Tuple2Tuple2) throws Exception {
                                //System.out.println(tuple2Tuple2Tuple2._1()._1() + "    :Hello-------------------");
                                //System.out.println(tuple2Tuple2Tuple2._2()._1() + "    :World-------------------");

                                return tuple2Tuple2Tuple2._1()._1().compareTo(tuple2Tuple2Tuple2._2()._1()) < 0;
                            }
                        }
                );

                List<Tuple2<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>>> a = cart.collect();

                System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxx" + cart2.collect());

            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}
