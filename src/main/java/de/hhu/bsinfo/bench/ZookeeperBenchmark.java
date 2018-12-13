package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxraft.client.ClientConfig;
import de.hhu.bsinfo.dxraft.client.RaftClient;
import de.hhu.bsinfo.dxraft.data.IntData;
import de.hhu.bsinfo.dxraft.util.ConfigUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public final class ZookeeperBenchmark {
    private static final Logger log = LogManager.getLogger();
    private static final String RESULT_FILE_PATH = "log/bench-result.csv";

    private ZookeeperBenchmark() {}

    public static void main(String[] p_args) throws IOException, KeeperException, InterruptedException {
        String resultPath = System.getProperty("bench.servers");
        if (resultPath == null) {
            resultPath = RESULT_FILE_PATH;
        }

        String servers = System.getProperty("bench.servers");
        if (servers == null) {
            log.error("Server list must be provided with -Dbench.servers");
            return;
        }

        if (p_args.length < 2) {
            log.error("Too few parameters. Parameters needed: [iteration count] [read percentage]");
            return;
        }

        int it_count = Integer.parseInt(p_args[0]);
        double read_percent = Double.parseDouble(p_args[1]);

        ZooKeeper zookeeper = new ZooKeeper(servers, 1000, null);
        zookeeper.create("/bench", null, null, CreateMode.PERSISTENT);

        long[] times = new long[it_count];

        int readMod = (int) (it_count / (read_percent * it_count));
        int progressMod = it_count / 20;
        long sum = 0;
        double avg = 0.0;
        Stat stat = null;

        log.info("Starting benchmark...");
        for (int i = 0; i < it_count; i++) {
            long start = System.nanoTime();
            if (i % readMod == 0) {
                zookeeper.getData("/bench", null, null);
            } else {
                stat = zookeeper.setData("/bench", new byte[] {1}, stat == null ? 0 : stat.getVersion());
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

        avg = sum / (double) it_count;

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
