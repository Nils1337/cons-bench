package de.hhu.bsinfo.bench;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZookeeperHandler implements ConsensusHandler {
    private static final Logger log = LogManager.getLogger(ZookeeperHandler.class);

    private ZooKeeper m_zookeeper;
    private Stat m_stat;

    @Override
    public boolean init() {
        String servers = System.getProperty("zookeeper.servers");
        if (servers == null) {
            log.error("Server list must be provided with -Dzookeeper.servers");
            return false;
        }

        try {
            m_zookeeper = new ZooKeeper(servers, 1000, null);
            m_stat = m_zookeeper.exists("/bench", null);
            if (m_stat == null) {
                m_zookeeper.create("/bench", new byte[]{1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
            m_zookeeper.getData("/bench", null, null);
        } catch (KeeperException | InterruptedException e) {
            log.error(e);
        }
    }

    @Override
    public void writeRequest() {
        try {
            m_stat = m_zookeeper.setData("/bench", new byte[] {1}, m_stat == null ? 0 : m_stat.getVersion());
        } catch (KeeperException | InterruptedException e) {
            log.error(e);
        }
    }
}
