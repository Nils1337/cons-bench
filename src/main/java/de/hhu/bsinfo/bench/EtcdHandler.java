package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;

public class EtcdHandler implements ConsensusHandler {
    private static final Logger LOG = LogManager.getLogger(ConsulHandler.class);
    private Client m_client;
    private KV m_kvClient;

    @Override
    public boolean init(int p_writeDist, StatisticsManager p_manager) {
        String servers = System.getProperty("servers");
        if (servers == null) {
            LOG.error("Server list must be provided with -Dservers");
            return false;
        }

        m_client = Client.builder().endpoints(servers).build();
        m_kvClient = m_client.getKVClient();
        return true;
    }

    @Override
    public void readRequest(String p_path) {
        ByteSequence key = ByteSequence.from(p_path, Charset.defaultCharset());
        CompletableFuture<GetResponse> future = m_kvClient.get(key);
        future.join();
    }

    @Override
    public void writeRequest(String p_path) {
        ByteSequence key = ByteSequence.from(p_path, Charset.defaultCharset());
        CompletableFuture<PutResponse> future = m_kvClient.put(key, ByteSequence.from("test",
                Charset.defaultCharset()));
        future.join();
    }

    @Override
    public void shutdown() {
        m_client.close();
    }
}
