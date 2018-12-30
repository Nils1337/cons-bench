package de.hhu.bsinfo.bench;

public interface ConsensusHandler {
    boolean init(int p_nodeCount);
    void readRequest(String p_path);
    void writeRequest(String p_path);
}
