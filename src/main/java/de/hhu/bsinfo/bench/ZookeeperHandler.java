package de.hhu.bsinfo.bench;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZookeeperHandler implements ConsensusHandler, Watcher {
    private static final Logger log = LogManager.getLogger(ZookeeperHandler.class);

    private ZooKeeper m_zookeeper;
    private Stat m_stat;
    private String m_path;

    @Override
    public boolean init(String p_path) {
        m_path = p_path;
        String servers = System.getProperty("zookeeper.servers");
        if (servers == null) {
            log.error("Server list must be provided with -Dzookeeper.servers");
            return false;
        }

        try {
            m_zookeeper = new ZooKeeper(servers, 1000, this);
            m_stat = m_zookeeper.exists(m_path, null);
            if (m_stat == null) {
                m_zookeeper.create(m_path, new byte[]{1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException | KeeperException | InterruptedException e) {
            log.error(e);
            return false;
        }
        return true;
    }

    @Override
    public void readRequest() {
        try {
            m_zookeeper.getData(m_path, null, null);
        } catch (KeeperException | InterruptedException e) {
            log.error(e);
        }
    }

    @Override
    public void writeRequest() {
        try {
            m_stat = m_zookeeper.setData(m_path, new byte[] {1}, m_stat == null ? 0 : m_stat.getVersion());
        } catch (KeeperException | InterruptedException e) {
            log.error(e);
        }
    }

    @Override
    public void process(WatchedEvent p_event) {

    }
}
