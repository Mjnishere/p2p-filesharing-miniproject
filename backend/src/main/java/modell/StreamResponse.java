package modell;

import java.util.ArrayList;
import java.util.List;

public class StreamResponse {
    private String type = "STREAM_RES";
    private String status; // OK or ERROR
    private String fileId;
    private long offset;
    private long length;
    private long crc32c; // Hardware-accelerated CRC32C checksum for window verification
    private List<Long> subBlockCrcs = new ArrayList<>(); // 64 KB sub-block CRC32C Merkle digests
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

    public static StreamResponse ok(String fileId, long offset, long length, long crc32c, List<Long> subBlockCrcs) {
        StreamResponse res = new StreamResponse("OK", fileId, offset, length);
        res.setCrc32c(crc32c);
        if (subBlockCrcs != null) {
            res.setSubBlockCrcs(subBlockCrcs);
        }
        return res;
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

    public long getCrc32c() { return crc32c; }
    public void setCrc32c(long crc32c) { this.crc32c = crc32c; }

    public List<Long> getSubBlockCrcs() { return subBlockCrcs; }
    public void setSubBlockCrcs(List<Long> subBlockCrcs) { this.subBlockCrcs = subBlockCrcs; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
