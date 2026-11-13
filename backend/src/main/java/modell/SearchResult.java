package modell;

public class SearchResult {
    private String fileId;
    private String fileName;
    private long sizeBytes;
    private String sourcePeerId;
    private String sourceHost;
    private int sourcePort;
    private String fileHash;
    private long rttMs;

    public SearchResult() {}

    public SearchResult(String fileId, String fileName, long sizeBytes, String sourcePeerId, String sourceHost, int sourcePort) {
        this.fileId = fileId;
        this.fileName = fileName;
        this.sizeBytes = sizeBytes;
        this.sourcePeerId = sourcePeerId;
        this.sourceHost = sourceHost;
        this.sourcePort = sourcePort;
    }

    public SearchResult(String fileId, String fileName, long sizeBytes, String sourcePeerId, String sourceHost, int sourcePort, String fileHash, long rttMs) {
        this.fileId = fileId;
        this.fileName = fileName;
        this.sizeBytes = sizeBytes;
        this.sourcePeerId = sourcePeerId;
        this.sourceHost = sourceHost;
        this.sourcePort = sourcePort;
        this.fileHash = fileHash;
        this.rttMs = rttMs;
    }

    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }

    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }

    public long getSizeBytes() { return sizeBytes; }
    public void setSizeBytes(long sizeBytes) { this.sizeBytes = sizeBytes; }

    public String getSourcePeerId() { return sourcePeerId; }
    public void setSourcePeerId(String sourcePeerId) { this.sourcePeerId = sourcePeerId; }

    public String getSourceHost() { return sourceHost; }
    public void setSourceHost(String sourceHost) { this.sourceHost = sourceHost; }

    public int getSourcePort() { return sourcePort; }
    public void setSourcePort(int sourcePort) { this.sourcePort = sourcePort; }

    public String getFileHash() { return fileHash; }
    public void setFileHash(String fileHash) { this.fileHash = fileHash; }

    public long getRttMs() { return rttMs; }
    public void setRttMs(long rttMs) { this.rttMs = rttMs; }
}
