package de.hhu.bsinfo.bench;

import de.hhu.bsinfo.dxutils.stats.StatisticsManager;

public interface ConsensusHandler {
    boolean init(int p_writeDist, StatisticsManager p_manager);
    void readRequest(String p_path);
    void writeRequest(String p_path);
    void shutdown();
}
