package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxutils.Stopwatch;
import de.hhu.bsinfo.dxutils.stats.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.io.*;
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
                    "[write distribution] [iteration count] [read percentage]");
            return;
        }

        StatisticsManager manager = StatisticsManager.get();
        ThroughputPool throughput = new ThroughputPool(Benchmark.class, "Throughput");
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

        int writeDist = Integer.parseInt(p_args[2]);

        if (!handler.init(writeDist)) {
            return;
        }

        int itCount = Integer.parseInt(p_args[3]);
        double readPercent = Double.parseDouble(p_args[4]);

        int readMod;
        if (readPercent != 0) {
            readMod = (int) (itCount / (readPercent * itCount));
        } else {
            readMod = itCount;
        }

        Thread[] threads = new Thread[threadCount];
        for (int j = 0; j < threadCount; j++) {
            threads[j] = new Thread(() -> {
                for (int i = 0; i < itCount; i++) {

                    int random = ThreadLocalRandom.current().nextInt(writeDist);
                    String path = "/bench-" + random;

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
                        long t = time.stop();
                        timePercentile.record(t);
                    }
                }
            });
        }

        manager.setPrintInterval(5000);

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

        log.info("Writing results to {}", resultPath);
        try {

            PrintStream stream = new PrintStream(new File(resultPath));
            manager.printStatisticTables(stream);
        } catch (FileNotFoundException e) {
            log.error(e);
        }
        manager.stopPeriodicPrinting();
        manager.interrupt();

        try {
            manager.join();
        } catch (InterruptedException e) {
            log.error(e);
        }

        handler.shutdown();
        log.info("Finished");
    }
}
