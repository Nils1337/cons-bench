package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;

public class ZookeeperHandler implements ConsensusHandler, Watcher {
    private static final Logger log = LogManager.getLogger(ZookeeperHandler.class);

    private ZooKeeper m_zookeeper;

    @Override
    public boolean init(int p_writeDist, StatisticsManager p_manager) {
        String servers = System.getProperty("zookeeper.servers");
        if (servers == null) {
            log.error("Server list must be provided with -Dzookeeper.servers");
            return false;
        }

        try {
            m_zookeeper = new ZooKeeper(servers, 1000, this);

            for (int i = 0; i < p_writeDist; i++) {
                if (m_zookeeper.exists("/bench-" + i, false) == null) {
                    m_zookeeper.create("/bench-" + i, new byte[]{1}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }

        } catch (IOException | KeeperException | InterruptedException e) {
            log.error(e);
            return false;
        }
        return true;
    }

    @Override
    public void readRequest(String p_path) {
        while (true) {
            try {
                m_zookeeper.getData(p_path, null, null);
                break;
            } catch (KeeperException.ConnectionLossException e) {
                log.warn(e);
            } catch (KeeperException | InterruptedException e) {
                log.error(e);
                break;
            }
        }
    }

    @Override
    public void writeRequest(String p_path) {
        while (true) {
            try {
                m_zookeeper.setData(p_path, new byte[] {1}, -1);
                break;
            } catch (KeeperException.ConnectionLossException e) {
                //try again
            } catch (KeeperException | InterruptedException e) {
                log.error(e);
                break;
            }
        }
    }

    @Override
    public void shutdown() {
        try {
            m_zookeeper.close();
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    @Override
    public void process(WatchedEvent p_event) {

    }
}
