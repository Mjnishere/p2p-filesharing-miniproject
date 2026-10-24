package modell;

public class ChunkRequest {
    private String fileId;
    private int chunkNo;

    public ChunkRequest() {}

    public ChunkRequest(String fileId, int chunkNo) {
        this.fileId = fileId;
        this.chunkNo = chunkNo;
    }

    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }

    public int getChunkNo() { return chunkNo; }
    public void setChunkNo(int chunkNo) { this.chunkNo = chunkNo; }
}
