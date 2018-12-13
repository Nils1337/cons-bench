package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxraft.client.ClientConfig;
import de.hhu.bsinfo.dxraft.client.RaftClient;
import de.hhu.bsinfo.dxraft.data.IntData;
import de.hhu.bsinfo.dxraft.data.StringData;
import de.hhu.bsinfo.dxraft.util.ConfigUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.stream.Stream;

public final class DXRaftBenchmark {
    private static final Logger log = LogManager.getLogger();

    private static final String CLIENT_CONFIG_PATH = "config/client-config.json";
    private static final String RESULT_FILE_PATH = "log/bench-result.csv";

    private DXRaftBenchmark() {}

    public static void main(String[] p_args) {
        String configPath = System.getProperty("dxraft.config");
        if (configPath == null) {
            configPath = CLIENT_CONFIG_PATH;
        }

        String resultPath = System.getProperty("dxraft.bench.result");
        if (resultPath == null) {
            resultPath = RESULT_FILE_PATH;
        }

        if (p_args.length < 2) {
            log.error("Too few parameters. Parameters needed: [iteration count] [read percentage]");
            return;
        }

        int it_count = Integer.parseInt(p_args[0]);
        double read_percent = Double.parseDouble(p_args[1]);

        ClientConfig config = ConfigUtils.getClientConfig(configPath);

        if (config == null) {
            return;
        }

        RaftClient client = new RaftClient(config);
        client.init();
        client.discoverServers();

        long[] times = new long[it_count];

        int readMod = (int) (it_count / (read_percent * it_count));
        int progressMod = it_count / 20;
        long sum = 0;
        double avg = 0.0;

        log.info("Starting benchmark...");
        for (int i = 0; i < it_count; i++) {
            long start = System.nanoTime();
            if (i % readMod == 0) {
                client.read("bench", false);
            } else {
                client.write("bench", new IntData(1), true);
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

