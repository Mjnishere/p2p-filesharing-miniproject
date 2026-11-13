package modell;

public class StreamResponse {
    private String type = "STREAM_RES";
    private String status; // OK or ERROR
    private String fileId;
    private long offset;
    private long length;
    private String errorMessage;

    public StreamResponse() {}

    public StreamResponse(String status, String fileId, long offset, long length) {
        this.type = "STREAM_RES";
        this.status = status;
        this.fileId = fileId;
        this.offset = offset;
        this.length = length;
    }

    public static StreamResponse ok(String fileId, long offset, long length) {
        return new StreamResponse("OK", fileId, offset, length);
    }

    public static StreamResponse error(String fileId, String errorMessage) {
        StreamResponse res = new StreamResponse();
        res.setStatus("ERROR");
        res.setFileId(fileId);
        res.setErrorMessage(errorMessage);
        return res;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public long getLength() { return length; }
    public void setLength(long length) { this.length = length; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
