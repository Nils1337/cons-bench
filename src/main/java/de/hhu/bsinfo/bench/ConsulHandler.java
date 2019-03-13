package de.hhu.bsinfo.bench;

import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ConsulHandler implements ConsensusHandler {
    private static final Logger LOG = LogManager.getLogger(ConsulHandler.class);

    private KeyValueClient m_kvClient;
    private Consul m_consul;

    @Override
    public boolean init(int p_writeDist) {
        String servers = System.getProperty("consul.servers");
        if (servers == null) {
            LOG.error("Server list must be provided with -Dzookeeper.servers");
            return false;
        }
        List<HostAndPort> serverList = Arrays.stream(servers.split(","))
                .map(HostAndPort::fromString).collect(Collectors.toList());
        if (serverList.size() > 1) {
            m_consul = Consul.builder().withMultipleHostAndPort(serverList, 1000).build();
        } else if (serverList.size() == 1) {
            m_consul = Consul.builder().withHostAndPort(serverList.get(0)).build();
        }
        m_kvClient = m_consul.keyValueClient();
        return true;
    }

    @Override
    public void readRequest(String p_path) {
        m_kvClient.getValue(p_path);
    }

    @Override
    public void writeRequest(String p_path) {
        m_kvClient.putValue(p_path, "bench");
    }

    @Override
    public void shutdown() {
        m_consul.destroy();
    }
}
