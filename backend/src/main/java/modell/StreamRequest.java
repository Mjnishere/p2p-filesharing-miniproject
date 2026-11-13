package modell;

public class StreamRequest {
    private String type = "STREAM_REQ";
    private String fileId;
    private String fileHash;
    private long offset;
    private long length;

    public StreamRequest() {}

    public StreamRequest(String fileId, String fileHash, long offset, long length) {
        this.type = "STREAM_REQ";
        this.fileId = fileId;
        this.fileHash = fileHash;
        this.offset = offset;
        this.length = length;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }

    public String getFileHash() { return fileHash; }
    public void setFileHash(String fileHash) { this.fileHash = fileHash; }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public long getLength() { return length; }
    public void setLength(long length) { this.length = length; }
}
