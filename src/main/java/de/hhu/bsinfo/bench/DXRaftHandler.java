package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxraft.client.ClientConfig;
import de.hhu.bsinfo.dxraft.client.RaftClient;
import de.hhu.bsinfo.dxraft.client.result.BooleanResult;
import de.hhu.bsinfo.dxraft.client.result.EntryResult;
import de.hhu.bsinfo.dxraft.data.IntData;
import de.hhu.bsinfo.dxraft.util.ConfigUtils;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePercentilePool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantLock;

public class DXRaftHandler implements ConsensusHandler {
    private static final Logger log = LogManager.getLogger(DXRaftHandler.class);

    private RaftClient m_raft;
    private boolean m_debugRequests;
    private TimePercentilePool m_acquireLockTime;
    private TimePercentilePool m_appendTime;
    private TimePercentilePool m_consensusTime;
    private TimePercentilePool m_firstFollowerSendTime;
    private TimePercentilePool m_majorityFollowerSendTime;

    private volatile BooleanResult m_longestRequestResponse;
    private volatile long m_longestTime = 0;
    private ReentrantLock m_lock = new ReentrantLock();

    public BooleanResult getLongestRequestResponse() {
        return m_longestRequestResponse;
    }

    public long getLongestTime() {
        return m_longestTime;
    }

    public DXRaftHandler(boolean p_debugRequests) {
        m_debugRequests = p_debugRequests;
        if (m_debugRequests) {
            m_acquireLockTime = new TimePercentilePool(DXRaftHandler.class, "Acquire Lock Time");
            m_appendTime = new TimePercentilePool(DXRaftHandler.class, "Log Append Time");
            m_consensusTime = new TimePercentilePool(DXRaftHandler.class, "Consensus Time");
            m_firstFollowerSendTime = new TimePercentilePool(DXRaftHandler.class, "Time until sent to first follower");
            m_majorityFollowerSendTime = new TimePercentilePool(DXRaftHandler.class, "Time until sent to majority of followers");
        }
    }

    @Override
    public boolean init(int p_writeDist, StatisticsManager p_manager) {
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

        if (m_debugRequests) {
            p_manager.registerOperation(DXRaftHandler.class, m_acquireLockTime);
            p_manager.registerOperation(DXRaftHandler.class, m_appendTime);
            p_manager.registerOperation(DXRaftHandler.class, m_consensusTime);
            p_manager.registerOperation(DXRaftHandler.class, m_firstFollowerSendTime);
            p_manager.registerOperation(DXRaftHandler.class, m_majorityFollowerSendTime);
        }

        return true;
    }

    @Override
    public void readRequest(String p_path) {
        EntryResult result = m_raft.read(p_path, false);
        if (result.getErrorCode() != 0) {
            log.error("DXRaft returned error {} when reading data", result.getErrorCode());
        }
    }

    @Override
    public void writeRequest(String p_path) {
        BooleanResult result;
        if (m_debugRequests) {
            long start = System.nanoTime();
            result = m_raft.write(p_path, new IntData(1), true, true);
            long time = System.nanoTime() - start;

            m_lock.lock();
            if (time > m_longestTime) {
                m_longestRequestResponse = result;
                m_longestTime = time;
            }
            m_lock.unlock();

            m_acquireLockTime.record(result.getAcquireLockTime());
            m_appendTime.record(result.getAppendTime());
            m_consensusTime.record(result.getConsensusTime());
            m_firstFollowerSendTime.record(result.getFirstFollowerSendTime());
            m_majorityFollowerSendTime.record(result.getMajorityFollowerSendTime());
        } else {
            result = m_raft.write(p_path, new IntData(1), true);
        }
        if (result.getErrorCode() != 0) {
            log.error("DXRaft returned error {} when writing data", result.getErrorCode());
        }
    }

    @Override
    public void shutdown() {
        m_raft.shutdown();
    }
}
