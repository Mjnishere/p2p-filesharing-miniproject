package modell;

public class NetworkMessage {
    private String type;
    private String senderPeerId;
    private String payload;

    public NetworkMessage() {}

    public NetworkMessage(String type, String senderPeerId, String payload) {
        this.type = type;
        this.senderPeerId = senderPeerId;
        this.payload = payload;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getSenderPeerId() { return senderPeerId; }
    public void setSenderPeerId(String senderPeerId) { this.senderPeerId = senderPeerId; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}