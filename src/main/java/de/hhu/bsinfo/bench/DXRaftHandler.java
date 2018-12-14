package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxraft.client.ClientConfig;
import de.hhu.bsinfo.dxraft.client.RaftClient;
import de.hhu.bsinfo.dxraft.data.IntData;
import de.hhu.bsinfo.dxraft.util.ConfigUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DXRaftHandler implements ConsensusHandler {
    private static final Logger log = LogManager.getLogger(DXRaftHandler.class);

    private RaftClient m_raft;

    @Override
    public boolean init() {
        String configPath = System.getProperty("dxraft.config");
        if (configPath == null) {
            log.error("Config path must be provided with -Ddxraft.config");
            return false;
        }

        ClientConfig config = ConfigUtils.getClientConfig(configPath);

        if (config == null) {
            return false;
        }

        m_raft = new RaftClient(config);
        boolean init = m_raft.init();

        if (!init) {
            return false;
        }

        m_raft.discoverServers();
        return true;
    }

    @Override
    public void readRequest() {
        m_raft.read("bench", false);
    }

    @Override
    public void writeRequest() {
        m_raft.write("bench", new IntData(1), true);
    }
}
