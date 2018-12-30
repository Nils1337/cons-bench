package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxutils.stats.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public final class Benchmark {
    private static final Logger log = LogManager.getLogger(Benchmark.class);

    private Benchmark() {}

    public static void main(String[] p_args) {
        String resultPath = System.getProperty("bench.result");
        if (resultPath == null) {
            log.error("Result path must be provided with -Dbench.result");
            return;
        }

        if (p_args.length < 5) {
            log.error("Too few parameters. Parameters needed: [benchmark type] [thread count] " +
                    "[node count] [iteration count] [read percentage]");
            return;
        }

        StatisticsManager manager = StatisticsManager.get();
        ThroughputPool throughput = new ThroughputPool(Benchmark.class, "Throughput", Value.Base.B_10);
        TimePool time = new TimePool(Benchmark.class, "Time");
        TimePercentilePool timePercentile = new TimePercentilePool(Benchmark.class, "Time Percentile");

        manager.registerOperation(Benchmark.class, throughput);
        manager.registerOperation(Benchmark.class, time);
        manager.registerOperation(Benchmark.class, timePercentile);

        ConsensusHandler handler;
        if ("z".equals(p_args[0]) || "zookeeper".equals(p_args[0])) {
            handler = new ZookeeperHandler();
        } else if ("r".equals(p_args[0]) || "dxraft".equals(p_args[0])) {
            handler = new DXRaftHandler();
        } else {
            log.error("Unknown benchmark type");
            return;
        }

        int threadCount = Integer.parseInt(p_args[1]);

        int nodeCount = Integer.parseInt(p_args[2]);

        if (!handler.init(nodeCount)) {
            return;
        }

        int itCount = Integer.parseInt(p_args[2]);
        double readPercent = Double.parseDouble(p_args[3]);
//        long[] times = new long[itCount];

        int readMod;
        if (readPercent != 0) {
            readMod = (int) (itCount / (readPercent * itCount));
        } else {
            readMod = itCount;
        }
//
//        int progressMod;
//        if (itCount >= 20) {
//            progressMod = itCount / 20;
//        } else {
//            progressMod = 20;
//        }
//
//        long sum = 0;
//        double avg = 0.0;

        Thread[] threads = new Thread[threadCount];
        for (int j = 0; j < threadCount; j++) {
            threads[j] = new Thread(() -> {
                for (int i = 0; i < itCount; i++) {
//                    long start = System.nanoTime();

                    int random = ThreadLocalRandom.current().nextInt(nodeCount);
                    String path = "bench-" + random;

                    if (i > itCount / 100) {
                        throughput.start();
                        time.start();
                    }

                    if (i % readMod == 0) {
                        handler.readRequest(path);
                    } else {
                        handler.writeRequest(path);
                    }

                    if (i > itCount / 100) {
                        throughput.stop(1);
                        long delta = time.stop();
                        timePercentile.record(delta);
                    }

//                    long end = System.nanoTime();
//                    long time = end - start;
//                    times[i] = time;
//                    sum += time;
//
//                    if (i != 0 && i % progressMod == 0) {
//                        avg = sum / (double) i;
//                        log.info( "{}% finished. Total time {} s. Current average {} ms.", i / progressMod * 5,
//                                String.format("%1$,.2f", sum / 1000000000.0), String.format("%1$,.2f", avg / 1000000));
//                    }
                }
            });
        }

        manager.start();

        for (int j = 0; j < threadCount; j++) {
            threads[j].start();
        }

        for (int j = 0; j < threadCount; j++) {
            try {
                threads[j].join();
            } catch (InterruptedException e) {
                log.error(e);
            }
        }

//        avg = sum / (double) itCount;
//
//        log.info("Benchmark finished. Total time was {} s. Average time was {} ms.", String.format("%1$,.2f",
//                sum / 1000000000.0), String.format("%1$,.2f", avg / 1000000));
//
//        log.info("Writing results to {}", resultPath);
//
//        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultPath))) {
//            for (long time : times) {
//                writer.write(String.valueOf(time));
//                writer.newLine();
//            }
//        } catch (IOException e) {
//            log.error("Writing to file failed", e);
//            return;
//        }

        manager.stopPeriodicPrinting();
        log.info("Finished");
    }
}
