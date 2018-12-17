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
    private String m_path;

    @Override
    public boolean init(String p_path) {
        m_path = p_path;
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
        m_raft.read(m_path, false);
    }

    @Override
    public void writeRequest() {
        m_raft.write(m_path, new IntData(1), true);
    }
}
