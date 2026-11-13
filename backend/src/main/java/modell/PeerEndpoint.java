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

    public PeerEndpoint() {}

    public PeerEndpoint(String peerId, String host, int port, long rttMs, boolean shareReady) {
        this.peerId = peerId;
        this.host = host;
        this.port = port;
        this.rttMs = rttMs;
        this.shareReady = shareReady;
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
    }

    public String getPeerId() { return peerId; }
    public void setPeerId(String peerId) { this.peerId = peerId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public long getRttMs() { return rttMs; }
    public void setRttMs(long rttMs) { this.rttMs = rttMs; }

    public boolean isShareReady() { return shareReady; }
    public void setShareReady(boolean shareReady) { this.shareReady = shareReady; }

    public List<Integer> getDataPorts() { return dataPorts; }
    public void setDataPorts(List<Integer> dataPorts) { this.dataPorts = dataPorts; }

    @Override
    public int compareTo(PeerEndpoint o) {
        if (o == null) return -1;
        int cmp = Long.compare(this.rttMs, o.rttMs);
        if (cmp != 0) return cmp;
        if (this.peerId != null && o.peerId != null) {
            return this.peerId.compareTo(o.peerId);
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
                ", shareReady=" + shareReady +
                '}';
    }
}
