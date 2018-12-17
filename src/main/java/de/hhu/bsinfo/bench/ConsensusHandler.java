package de.hhu.bsinfo.bench;

public interface ConsensusHandler {
    boolean init(String path);
    void readRequest();
    void writeRequest();
}
