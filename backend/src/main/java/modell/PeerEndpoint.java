package modell;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PeerEndpoint implements Comparable<PeerEndpoint> {
    private String peerId;
    private String host;
    private int port;
    private long rttMs;
    private boolean shareReady;
    private List<Integer> dataPorts = new ArrayList<>();
    private double bandwidthBps = 0.0; // EWMA estimated bandwidth in bytes/sec
    private double utilityScore = 0.0; // Composite BDP/RTT routing score

    public PeerEndpoint() {}

    public PeerEndpoint(String peerId, String host, int port, long rttMs, boolean shareReady) {
        this.peerId = peerId;
        this.host = host;
        this.port = port;
        this.rttMs = rttMs;
        this.shareReady = shareReady;
        recalculateUtilityScore();
    }

    public PeerEndpoint(String peerId, String host, int port, long rttMs, boolean shareReady, List<Integer> dataPorts) {
        this.peerId = peerId;
        this.host = host;
        this.port = port;
        this.rttMs = rttMs;
        this.shareReady = shareReady;
        if (dataPorts != null) {
            this.dataPorts = dataPorts;
        }
        recalculateUtilityScore();
    }

    public void updateEwmaMetrics(long sampleRttMs, double sampleBandwidthBps) {
        if (sampleRttMs > 0) {
            this.rttMs = (this.rttMs <= 0) ? sampleRttMs : Math.max(1L, Math.round(0.7 * this.rttMs + 0.3 * sampleRttMs));
        }
        if (sampleBandwidthBps > 0) {
            this.bandwidthBps = (this.bandwidthBps <= 0)
                    ? sampleBandwidthBps
                    : (0.7 * this.bandwidthBps + 0.3 * sampleBandwidthBps);
        }
        recalculateUtilityScore();
    }

    public void recalculateUtilityScore() {
        double effectiveRtt = Math.max(1.0, this.rttMs <= 0 ? 5.0 : this.rttMs);
        double bwKb = this.bandwidthBps > 0 ? (this.bandwidthBps / 1024.0) : 10240.0; // Default 10 MB/s baseline
        this.utilityScore = bwKb / effectiveRtt;
    }

    public String getPeerId() { return peerId; }
    public void setPeerId(String peerId) { this.peerId = peerId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public long getRttMs() { return rttMs; }
    public void setRttMs(long rttMs) {
        this.rttMs = rttMs;
        recalculateUtilityScore();
    }

    public boolean isShareReady() { return shareReady; }
    public void setShareReady(boolean shareReady) { this.shareReady = shareReady; }

    public List<Integer> getDataPorts() { return dataPorts; }
    public void setDataPorts(List<Integer> dataPorts) { this.dataPorts = dataPorts; }

    public double getBandwidthBps() { return bandwidthBps; }
    public void setBandwidthBps(double bandwidthBps) {
        this.bandwidthBps = bandwidthBps;
        recalculateUtilityScore();
    }

    public double getUtilityScore() { return utilityScore; }
    public void setUtilityScore(double utilityScore) { this.utilityScore = utilityScore; }

    @Override
    public int compareTo(PeerEndpoint o) {
        if (o == null) return -1;
        // Primary ordering: lowest positive RTT first (preserves deterministic RTT ordering)
        int cmp = Long.compare(this.rttMs, o.rttMs);
        if (cmp != 0) return cmp;
        // Tie-breaker: higher EWMA bandwidth first
        int bwCmp = Double.compare(o.bandwidthBps, this.bandwidthBps);
        if (bwCmp != 0) return bwCmp;
        if (this.peerId != null && o.peerId != null) {
            int idCmp = this.peerId.compareTo(o.peerId);
            if (idCmp != 0) return idCmp;
        }
        return Integer.compare(this.port, o.port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PeerEndpoint that)) return false;
        return port == that.port &&
                Objects.equals(peerId, that.peerId) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(peerId, host, port);
    }

    @Override
    public String toString() {
        return "PeerEndpoint{" +
                "peerId='" + peerId + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", rttMs=" + rttMs +
                ", bandwidthBps=" + Math.round(bandwidthBps) +
                ", shareReady=" + shareReady +
                '}';
    }
}
