package ncdc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ncdc.ISH.ISHBasic;
import scala.Tuple2;

/**
 * @author Ali Shakiba
 */
public class Spark {

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("ERROR: Invalid arguments!");
            return;
        }

        String intpuFiles = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf().setAppName("NCDC");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long startTime = System.currentTimeMillis();

        // read input
        JavaRDD<String> input = sc.textFile(intpuFiles);

        // parse input
        JavaRDD<ISHBasic> data = input.map(line -> ISH.parseBasic(line))
                .filter(d -> d != null);

        // <USAF YR MO, 1>
        JavaPairRDD<String, Integer> operable = data
                .mapToPair(d -> pair(d.usaf + d.year + d.month, 1));
        operable = operable.reduceByKey((m, n) -> 1);

        // <USAF YR, count(months)>
        operable = operable.mapToPair(d -> pair(d._1.substring(0, 10), 1));
        operable = operable.reduceByKey((m, n) -> (m + n));

        // print <USAF, ["YR: months"]>
        // operable.mapToPair(d -> pair(d._1.substring(0, 6),
        // d._1.substring(6, 10) + ": " + d._2)).groupByKey()
        // .collect().forEach(d -> System.out.println(d));

        // keep only operable (active 12 months a year)
        operable = operable.filter(d -> d._2 == 12);

        // <USAF, sorted List<YR>>
        JavaPairRDD<String, List<Integer>> stationYears = operable
                .mapToPair(
                        d -> pair(d._1.substring(0, 6), d._1.substring(6, 10)))
                .groupByKey().mapToPair(d -> pair(d._1, sortedList(d._2)));

        // write top active years
        stationYears.map(d -> d._1 + ": " + d._2)
                .saveAsTextFile(outputPath + "/active_years");

        // sorted <number of active years, USAF>
        JavaPairRDD<Integer, String> topActive = stationYears
                .mapToPair(d -> pair(d._2.size(), d._1)).sortByKey(false);
        // write top active years
        topActive.map(d -> d._2 + "," + d._1)
                .saveAsTextFile(outputPath + "/top_active");

        // top 100 active
        // topActive.take(100)
        // .forEach(d -> System.out.println(d._2 + ": " + d._1));

        // sorted <number of consecutive active years, USAF>
        JavaPairRDD<Integer, String> topConsActive = stationYears
                .mapToPair(d -> pair(maxConsecutive(d._2), d._1))
                .sortByKey(false);
        // write top consecutive active years
        topConsActive.map(d -> d._2 + "," + d._1)
                .saveAsTextFile(outputPath + "/top_cons_active");

        // only top 50 consecutive active
        // topConsActive.take(50)
        // .forEach(d -> System.out.println(d._2 + ": " + d._1));

        // applying filter using inner join
        data = data.mapToPair(d -> pair(d.usaf + d.year, d)).join(operable)
                .map(d -> d._2._1);

        // write filtered data
        data.map(d -> d.usaf + "," + d.year + "," + d.month + "," + d.day + ","
                + d.hour + "," + d.temp + "," + d.dewp + "," + d.pressure + ","
                + d.windSpeed + "," + d.windDirection)
                .saveAsTextFile(outputPath + "/operable");

        System.out.println(data.count());
        System.out.println(System.currentTimeMillis() - startTime + "ms");

        sc.close();
    }

    private static <K, V> Tuple2<K, V> pair(K k, V v) {
        return new Tuple2<K, V>(k, v);
    }

    private static List<Integer> sortedList(Iterable<String> numbers) {
        List<Integer> sorted = new ArrayList<Integer>();
        for (String year : numbers) {
            sorted.add(Integer.parseInt(year));
        }
        sorted.sort(null);
        return sorted;
    }

    // not required for sorted lists
    // http://www.geeksforgeeks.org/longest-consecutive-subsequence/
    private static int maxConsecutive(List<Integer> nums) {
        HashSet<Integer> hashset = new HashSet<Integer>();
        int max = 0;
        for (int num : nums) {
            hashset.add(num);
        }
        for (int num : nums) {
            if (!hashset.contains(num - 1)) {
                int j = num;
                while (hashset.contains(j)) {
                    j++;
                }
                if (max < j - num) {
                    max = j - num;
                }
            }
        }
        return max;
    }

}