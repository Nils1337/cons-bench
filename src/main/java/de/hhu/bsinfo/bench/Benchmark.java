package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxraft.client.result.BooleanResult;
import de.hhu.bsinfo.dxraft.server.message.RequestResponse;
import de.hhu.bsinfo.dxutils.stats.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

        String historyPath = System.getProperty("history");

        if (p_args.length < 5) {
            log.error("Too few parameters. Parameters needed: [benchmark type] [thread count] " +
                    "[write distribution] [iteration count] [read percentage]");
            return;
        }

        boolean debugRequests = false;
        String debug = System.getProperty("debug-requests");
        if (debug != null) {
            debugRequests = true;
        }

        StatisticsManager manager = StatisticsManager.get();
        ThroughputPool throughput = new ThroughputPool(Benchmark.class, "Throughput");
        TimePool time = new TimePool(Benchmark.class, "Time");
        TimePercentilePool timePercentile = new TimePercentilePool(Benchmark.class, "Time Percentile");
        ValueHistory history = new ValueHistory(Benchmark.class, "History");

        manager.registerOperation(Benchmark.class, throughput);
        manager.registerOperation(Benchmark.class, time);
        manager.registerOperation(Benchmark.class, timePercentile);
        manager.setExtended(true);

        ConsensusHandler handler;
        if ("z".equals(p_args[0]) || "zookeeper".equals(p_args[0])) {
            handler = new ZookeeperHandler();
        } else if ("r".equals(p_args[0]) || "dxraft".equals(p_args[0])) {
            handler = new DXRaftHandler(debugRequests);
        } else if ("c".equals(p_args[0]) || "consul".equals(p_args[0])) {
            handler = new ConsulHandler();
        } else if ("e".equals(p_args[0]) || "etcd".equals(p_args[0])) {
            handler = new EtcdHandler();
        } else {
            log.error("Unknown benchmark type");
            return;
        }

        int threadCount = Integer.parseInt(p_args[1]);

        int writeDist = Integer.parseInt(p_args[2]);

        if (!handler.init(writeDist, manager)) {
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

        long start = System.nanoTime();

        Thread[] threads = new Thread[threadCount];
        for (int j = 0; j < threadCount; j++) {
            threads[j] = new Thread(() -> {
                for (int i = 1; i <= itCount; i++) {

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
                        long timestamp = System.nanoTime();
                        timePercentile.record(t);
                        history.record(timestamp - start, t);
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

        manager.stopPeriodicPrinting();
        manager.interrupt();

        try {
            manager.join();
        } catch (InterruptedException e) {
            log.error(e);
        }

        if (debugRequests && handler instanceof DXRaftHandler) {
            Time totalTime = new Time(DXRaftHandler.class, "Total Time of longest request");
            Time acquireLockTime = new Time(DXRaftHandler.class, "Maximum Acquire Lock Time");
            Time appendTime = new Time(DXRaftHandler.class, "Maximum Log Append Time");
            Time consensusTime = new Time(DXRaftHandler.class, "Maximum Consensus Time");
            Time firstFollowerSendTime = new Time(DXRaftHandler.class, "Maximum Time until sent to first follower");
            Time majorityFollowerSendTime = new Time(DXRaftHandler.class, "Maximum Time until sent to majority of followers");

            BooleanResult longestRequestResponse = ((DXRaftHandler) handler).getLongestRequestResponse();
            totalTime.add(((DXRaftHandler) handler).getLongestTime());
            acquireLockTime.add(longestRequestResponse.getAcquireLockTime());
            appendTime.add(longestRequestResponse.getAppendTime());
            consensusTime.add(longestRequestResponse.getConsensusTime());
            firstFollowerSendTime.add(longestRequestResponse.getFirstFollowerSendTime());
            majorityFollowerSendTime.add(longestRequestResponse.getMajorityFollowerSendTime());

            manager.registerOperation(DXRaftHandler.class, totalTime);
            manager.registerOperation(DXRaftHandler.class, acquireLockTime);
            manager.registerOperation(DXRaftHandler.class, appendTime);
            manager.registerOperation(DXRaftHandler.class, consensusTime);
            manager.registerOperation(DXRaftHandler.class, firstFollowerSendTime);
            manager.registerOperation(DXRaftHandler.class, majorityFollowerSendTime);
        }

        log.info("Writing results to {}", resultPath);
        try {
            PrintStream stream = new PrintStream(new File(resultPath));
            manager.printStatisticTables(stream);
        } catch (FileNotFoundException e) {
            log.error(e);
        }

        if (historyPath != null) {
            log.info("Writing history to {}", historyPath);
            try {
                PrintStream stream = new PrintStream(new File(historyPath));
                String csv = history.generateCSVHeader(';') + '\n';
                csv += history.toCSV(';') + '\n';
                stream.print(csv);
            } catch (FileNotFoundException e) {
                log.error(e);
            }
        }


        handler.shutdown();
        log.info("Finished");
    }
}
