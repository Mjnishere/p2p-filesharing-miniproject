package modell;

public class ChunkData {
    private String fileId;
    private int chunkNo;
    private String dataBase64;
    private long sizeBytes;
    private boolean success;
    private String errorMessage;

    public ChunkData() {}

    public ChunkData(String fileId, int chunkNo, String dataBase64, long sizeBytes) {
        this.fileId = fileId;
        this.chunkNo = chunkNo;
        this.dataBase64 = dataBase64;
        this.sizeBytes = sizeBytes;
        this.success = true;
    }

    public static ChunkData error(String fileId, int chunkNo, String errorMessage) {
        ChunkData c = new ChunkData();
        c.setFileId(fileId);
        c.setChunkNo(chunkNo);
        c.setSuccess(false);
        c.setErrorMessage(errorMessage);
        return c;
    }

    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }

    public int getChunkNo() { return chunkNo; }
    public void setChunkNo(int chunkNo) { this.chunkNo = chunkNo; }

    public String getDataBase64() { return dataBase64; }
    public void setDataBase64(String dataBase64) { this.dataBase64 = dataBase64; }

    public long getSizeBytes() { return sizeBytes; }
    public void setSizeBytes(long sizeBytes) { this.sizeBytes = sizeBytes; }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
