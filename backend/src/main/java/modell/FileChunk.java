package modell;

public class FileChunk {
    private String chunkId;
    private String fileId;
    private int chunkNo;
    private long sizeBytes;
    private String path;

    public FileChunk(String chunkId, String fileId, int chunkNo, long sizeBytes, String path) {
        this.chunkId = chunkId;
        this.fileId = fileId;
        this.chunkNo = chunkNo;
        this.sizeBytes = sizeBytes;
        this.path = path;
    }

    public String getChunkId() { return chunkId; }
    public String getFileId() { return fileId; }
    public int getChunkNo() { return chunkNo; }
    public long getSizeBytes() { return sizeBytes; }
    public String getPath() { return path; }
}