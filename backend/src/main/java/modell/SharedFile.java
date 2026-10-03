package modell;

public class SharedFile {
    private String fileId;
    private String name;
    private long sizeBytes;
    private String ownerPeerId;
    private String originalPath;

    public SharedFile(String fileId, String name, long sizeBytes, String ownerPeerId, String originalPath) {
        this.fileId = fileId;
        this.name = name;
        this.sizeBytes = sizeBytes;
        this.ownerPeerId = ownerPeerId;
        this.originalPath = originalPath;
    }

    public String getFileId() { return fileId; }
    public String getName() { return name; }
    public long getSizeBytes() { return sizeBytes; }
    public String getOwnerPeerId() { return ownerPeerId; }
    public String getOriginalPath() { return originalPath; }
}