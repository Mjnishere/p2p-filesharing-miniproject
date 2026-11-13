package modell;

import java.util.ArrayList;
import java.util.List;

public class PeerInfo {
    private String peerId;
    private String host;
    private int port;
    private long rttMs;
    private boolean shareReady = true;
    private List<Integer> dataPorts = new ArrayList<>();

    public PeerInfo() {}

    public PeerInfo(String peerId, String host, int port) {
        this.peerId = peerId;
        this.host = host;
        this.port = port;
    }

    public PeerInfo(String peerId, String host, int port, long rttMs, boolean shareReady) {
        this.peerId = peerId;
        this.host = host;
        this.port = port;
        this.rttMs = rttMs;
        this.shareReady = shareReady;
    }

    public PeerInfo(String peerId, String host, int port, long rttMs, boolean shareReady, List<Integer> dataPorts) {
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

    public PeerEndpoint toEndpoint() {
        return new PeerEndpoint(peerId, host, port, rttMs, shareReady, dataPorts);
    }
}
