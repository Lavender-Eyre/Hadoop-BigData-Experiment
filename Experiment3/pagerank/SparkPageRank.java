import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/** Java 8 RDD PageRank: run with spark-submit --master yarn. */
public final class SparkPageRank {
    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: SparkPageRank HDFS_NORMALIZED_TSV HDFS_OUTPUT_DIR");
        }
        SparkConf conf = new SparkConf().setAppName("Lab3-Spark-PageRank-10-iterations");
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaPairRDD<String, List<String>> links = context.textFile(args[0], 2)
                .filter(line -> !line.isEmpty())
                .mapToPair(line -> {
                    String[] parts = line.split("\t", -1);
                    if (parts.length != 2 || parts[0].isEmpty()) {
                        throw new IllegalArgumentException("Use the normalized.tsv produced by normalize.py");
                    }
                    List<String> targets = parts[1].isEmpty()
                        ? Collections.<String>emptyList()
                        : Arrays.asList(parts[1].split(","));
                    return new Tuple2<String, List<String>>(parts[0], targets);
                }).cache();
            final long n = links.count();
            if (n == 0) throw new IllegalArgumentException("Empty graph");
            JavaPairRDD<String, Double> ranks = links.mapValues(targets -> 1.0).cache();
            ranks.count();
            for (int iteration = 1; iteration <= 10; iteration++) {
                JavaPairRDD<String, Tuple2<List<String>, Double>> joined = links.join(ranks).cache();
                final double dangling = joined.values()
                    .filter(value -> value._1().isEmpty())
                    .map(value -> value._2()).fold(0.0, (a, b) -> a + b);
                JavaPairRDD<String, Double> contributions = joined.values().flatMapToPair(value -> {
                    List<Tuple2<String, Double>> result = new ArrayList<>();
                    List<String> targets = value._1();
                    if (!targets.isEmpty()) {
                        double share = value._2() / targets.size();
                        for (String target : targets) result.add(new Tuple2<>(target, share));
                    }
                    return result.iterator();
                }).reduceByKey((a, b) -> a + b);
                JavaPairRDD<String, Double> next = links.leftOuterJoin(contributions)
                    .mapValues(value -> 0.15 + 0.85 * (value._2().orElse(0.0) + dangling / n))
                    .cache();
                // Materialize this iteration before releasing its parents.
                double total = next.values().fold(0.0, (a, b) -> a + b);
                System.out.printf(Locale.ROOT, "Iteration %02d: pages=%d, rank_sum=%.12f%n",
                                  iteration, n, total);
                ranks.unpersist();
                joined.unpersist();
                ranks = next;
            }
            ranks.sortByKey(true, 1)
                .map(item -> String.format(Locale.ROOT, "(%s,%.10f)", item._1(), item._2()))
                .saveAsTextFile(args[1]);
            System.out.println("Completed exactly 10 PageRank iterations. Output: " + args[1]);
            ranks.unpersist();
            links.unpersist();
        }
    }
}
