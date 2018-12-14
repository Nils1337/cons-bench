package de.hhu.bsinfo.bench;

public interface ConsensusHandler {
    boolean init();
    void readRequest();
    void writeRequest();
}
