package de.hhu.bsinfo.bench;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public final class Benchmark {
    private static final Logger log = LogManager.getLogger(Benchmark.class);

    private Benchmark() {}

    public static void main(String[] p_args) {
        String resultPath = System.getProperty("bench.result");
        if (resultPath == null) {
            log.error("Result path must be provided with -Dbench.result");
            return;
        }

        if (p_args.length < 3) {
            log.error("Too few parameters. Parameters needed: [benchmark type] [iteration count] [read percentage]");
            return;
        }

        ConsensusHandler handler;
        if ("z".equals(p_args[0]) || "zookeeper".equals(p_args[0])) {
            handler = new ZookeeperHandler();
        } else if ("r".equals(p_args[0]) || "dxraft".equals(p_args[0])) {
            handler = new DXRaftHandler();
        } else {
            log.error("Unknown benchmark type");
            return;
        }

        if (!handler.init()) {
            return;
        }

        int itCount = Integer.parseInt(p_args[1]);
        double readPercent = Double.parseDouble(p_args[2]);
        long[] times = new long[itCount];

        int readMod = (int) (itCount / (readPercent * itCount));
        int progressMod = itCount / 20;
        long sum = 0;
        double avg = 0.0;
        Stat stat = null;

        log.info("Starting benchmark...");
        for (int i = 0; i < itCount; i++) {
            long start = System.nanoTime();
            if (i % readMod == 0) {
                handler.readRequest();
            } else {
                handler.writeRequest();
            }
            long end = System.nanoTime();
            long time = end - start;
            times[i] = time;
            sum += time;

            if (i != 0 && i % progressMod == 0) {
                avg = sum / (double) i;
                log.info( "{}% finished. Total time {} s. Current average {} ms.", i / progressMod * 5,
                        String.format("%1$,.2f", sum / 1000000000.0), String.format("%1$,.2f", avg / 1000000));
            }
        }

        avg = sum / (double) itCount;

        log.info("Benchmark finished. Total time was {} ms. Average time was {} ms.", String.format("%1$,.2f",
                sum / 1000000000.0), String.format("%1$,.2f", avg / 1000000));

        log.info("Writing results to {}", resultPath);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(resultPath))) {
            for (long time : times) {
                writer.write(String.valueOf(time));
                writer.newLine();
            }
        } catch (IOException e) {
            log.error("Writing to file failed", e);
            return;
        }

        log.info("Finished");
    }
}
