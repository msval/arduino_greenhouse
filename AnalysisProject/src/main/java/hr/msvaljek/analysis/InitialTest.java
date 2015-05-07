package hr.msvaljek.analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class InitialTest implements Serializable {

    private transient SparkConf conf;

    private InitialTest(SparkConf conf) {
        this.conf = conf;
    }

    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        compute(sc);
    }

    private void compute(JavaSparkContext sc) {
        // load the measurements
        JavaRDD<Measurement> measurements = javaFunctions(sc)
                .cassandraTable("home", "greenhouse", mapRowTo(Measurement.class));

        measurements.persist(StorageLevel.MEMORY_AND_DISK());

        // total count
        long count = measurements.count();
        System.out.println("\nTotal Count");
        System.out.println(count);

        // sort the records by time
        List <Measurement> sortedTimestamps = measurements.collect();
        sortedTimestamps.sort(new Comparator<Measurement>() {
            @Override
            public int compare(Measurement o1, Measurement o2) {
                return o1.getTime().compareTo(o2.getTime());
            }
        });

        // print first and last record
        System.out.println("\nFirst record");
        System.out.println(sortedTimestamps.get(0));

        System.out.println("\nLast record");
        System.out.println(sortedTimestamps.get(sortedTimestamps.size() - 1));

        // get the cold nights
        final BigDecimal coldNightMinTemperature = new BigDecimal(2);
        JavaRDD<Measurement> coldMeasurements = measurements.filter(new Function<Measurement, Boolean>() {
            public Boolean call(Measurement x) {
                return x.getTemperatureout().compareTo(coldNightMinTemperature) <= 0;
            }
        });

        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        JavaRDD<String> coldNights = coldMeasurements.map(new Function<Measurement, String>() {
            @Override
            public String call(Measurement m) throws Exception {
                return dateFormat.format(m.getTime());
            }
        });

        JavaRDD<String> coldNightsDistinct = coldNights.distinct();

        System.out.println("\nCold nights (bellow 2 C)");
        for (String night : coldNightsDistinct.collect()) {
            System.out.println(night);
        }

        // lowest in temperature
        Measurement lowestIn = measurements.fold(sortedTimestamps.get(0),
                new Function2<Measurement, Measurement, Measurement>() {
            @Override
            public Measurement call(Measurement v1, Measurement v2) throws Exception {
                return v1.getTemperaturein().compareTo(v2.getTemperaturein()) <= 0 ? v1 : v2;
            }
        });
        System.out.println("\nLowest In");
        System.out.println(lowestIn);

        // highest in temperature
        Measurement hihgestIn = measurements.fold(sortedTimestamps.get(0),
                new Function2<Measurement, Measurement, Measurement>() {
                    @Override
                    public Measurement call(Measurement v1, Measurement v2) throws Exception {
                        return v1.getTemperaturein().compareTo(v2.getTemperaturein()) <= 0 ? v2 : v1;
                    }
                });
        System.out.println("\nHighest In");
        System.out.println(hihgestIn);

        // average in temperature
        Function2<AvgCount, Measurement, AvgCount> addAndCountIn =
                new Function2<AvgCount, Measurement, AvgCount>() {
                    public AvgCount call(AvgCount a, Measurement x) {
                        a.total = a.total.add(x.getTemperaturein());
                        a.num += 1;
                        return a;
                    }
                };
        Function2<AvgCount, AvgCount, AvgCount> combine =
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount a, AvgCount b) {
                        a.total = a.total.add(b.total);
                        a.num += b.num;
                        return a;
                    }
                };

        AvgCount initial = new AvgCount(BigDecimal.ZERO, 0);

        AvgCount result = measurements.aggregate(initial, addAndCountIn, combine);

        System.out.println("\nAverage In");
        System.out.println(result.avg());

        // lowest out temperature
        Measurement lowestOut = measurements.fold(sortedTimestamps.get(0),
                new Function2<Measurement, Measurement, Measurement>() {
                    @Override
                    public Measurement call(Measurement v1, Measurement v2) throws Exception {
                        return v1.getTemperatureout().compareTo(v2.getTemperatureout()) <= 0 ? v1 : v2;
                    }
                });
        System.out.println("\nLowest Out");
        System.out.println(lowestOut);

        // highest out temperature
        Measurement hihgestOut = measurements.fold(sortedTimestamps.get(0),
                new Function2<Measurement, Measurement, Measurement>() {
                    @Override
                    public Measurement call(Measurement v1, Measurement v2) throws Exception {
                        return v1.getTemperatureout().compareTo(v2.getTemperatureout()) <= 0 ? v2 : v1;
                    }
                });
        System.out.println("\nHighest Out");
        System.out.println(hihgestOut);

        // average out temperature
        Function2<AvgCount, Measurement, AvgCount> addAndCountOut =
                new Function2<AvgCount, Measurement, AvgCount>() {
                    public AvgCount call(AvgCount a, Measurement x) {
                        a.total = a.total.add(x.getTemperatureout());
                        a.num += 1;
                        return a;
                    }
                };

        AvgCount resultOut = measurements.aggregate(initial, addAndCountOut, combine);

        System.out.println("\nAverage Out");
        System.out.println(resultOut.avg());

        // average difference in and out
        JavaRDD<BigDecimal> differences = measurements.map(new Function<Measurement, BigDecimal>() {
            @Override
            public BigDecimal call(Measurement v1) throws Exception {
                return v1.getTemperaturein().subtract(v1.getTemperatureout());
            }
        });

        BigDecimal sumDifferences = differences.reduce(new Function2<BigDecimal, BigDecimal, BigDecimal>() {
            @Override
            public BigDecimal call(BigDecimal v1, BigDecimal v2) throws Exception {
                return v1.add(v2);
            }
        });

        System.out.println("\nAverage Difference");
        System.out.println(sumDifferences.divide(new BigDecimal(count), BigDecimal.ROUND_UP));


        // biggest difference
        Measurement biggestDifference = measurements.fold(sortedTimestamps.get(0),
                new Function2<Measurement, Measurement, Measurement>() {
                    @Override
                    public Measurement call(Measurement v1, Measurement v2) throws Exception {
                        BigDecimal diff1 = v1.getTemperaturein().subtract(v1.getTemperatureout());
                        BigDecimal diff2 = v2.getTemperaturein().subtract(v2.getTemperatureout());

                        return diff1.compareTo(diff2) <= 0 ? v2 : v1;
                    }
                });
        System.out.println("\nBiggest Diff");
        System.out.println(biggestDifference);
    }

    public static void main (String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("InitialTest");
        conf.setMaster("local"); //spark://mm-marko.lan:7077
        conf.set("spark.cassandra.connection.host", "192.168.1.10");

        InitialTest app = new InitialTest(conf);
        app.run();

    }

    public static class Measurement implements Serializable {
        private String source;
        private String day;
        private Date time;
        private BigDecimal temperaturein;
        private BigDecimal temperatureout;
        private BigDecimal temperaturecheck;
        private BigDecimal humidity;
        private Integer light;

        public Measurement() {

        }

        public Measurement(String source, String day, Date time, BigDecimal temperaturein,
                           BigDecimal temperatureout, BigDecimal temperaturecheck, BigDecimal humidity, Integer light) {
            this.source = source;
            this.day = day;
            this.time = time;
            this.temperaturein = temperaturein;
            this.temperatureout = temperatureout;
            this.temperaturecheck = temperaturecheck;
            this.humidity = humidity;
            this.light = light;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getDay() {
            return day;
        }

        public void setDay(String day) {
            this.day = day;
        }

        public Date getTime() {
            return time;
        }

        public void setTime(Date time) {
            this.time = time;
        }

        public BigDecimal getTemperaturein() {
            return temperaturein;
        }

        public void setTemperaturein(BigDecimal temperaturein) {
            this.temperaturein = temperaturein;
        }

        public BigDecimal getTemperatureout() {
            return temperatureout;
        }

        public void setTemperatureout(BigDecimal temperatureout) {
            this.temperatureout = temperatureout;
        }

        public BigDecimal getTemperaturecheck() {
            return temperaturecheck;
        }

        public void setTemperaturecheck(BigDecimal temperaturecheck) {
            this.temperaturecheck = temperaturecheck;
        }

        public BigDecimal getHumidity() {
            return humidity;
        }

        public void setHumidity(BigDecimal humidity) {
            this.humidity = humidity;
        }

        public Integer getLight() {
            return light;
        }

        public void setLight(Integer light) {
            this.light = light;
        }

        @Override
        public String toString() {
            return "Measurement{" +
                    "source='" + source + '\'' +
                    ", day='" + day + '\'' +
                    ", time=" + time +
                    ", temperaturein=" + temperaturein +
                    ", temperatureout=" + temperatureout +
                    ", temperaturecheck=" + temperaturecheck +
                    ", humidity=" + humidity +
                    ", light=" + light +
                    '}';
        }
    }

    private static class AvgCount implements Serializable {
        public BigDecimal total;
        public int num;

        public AvgCount(BigDecimal total, int num) {
            this.total = total;
            this.num = num;
        }

        public BigDecimal avg() {
            return total.divide(new BigDecimal(num), BigDecimal.ROUND_UP);
        }
    }
}
