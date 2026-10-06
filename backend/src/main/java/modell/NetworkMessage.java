package modell;

public class NetworkMessage {
    private String type;         // HELLO, HELLO_ACK, PEER_LIST, SEARCH, SEARCH_RESULT, CHUNK_REQUEST, CHUNK_RESPONSE
    private String requestId;    // Unique UUID for duplicate suppression (Slide 4 & 8)
    private String originPeerId; // The peer that initiated the search/request
    private String senderPeerId; // Direct peer sending this hop
    private int ttl;             // Time-To-Live counter (decremented each hop)
    private String payload;      // JSON payload (file keyword, chunk index, peer lists, etc.)

    public NetworkMessage() {}

    public NetworkMessage(String type, String senderPeerId, String payload) {
        this.type = type;
        this.senderPeerId = senderPeerId;
        this.payload = payload;
    }

    public NetworkMessage(String type, String requestId, String originPeerId, String senderPeerId, int ttl, String payload) {
        this.type = type;
        this.requestId = requestId;
        this.originPeerId = originPeerId;
        this.senderPeerId = senderPeerId;
        this.ttl = ttl;
        this.payload = payload;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public String getOriginPeerId() { return originPeerId; }
    public void setOriginPeerId(String originPeerId) { this.originPeerId = originPeerId; }

    public String getSenderPeerId() { return senderPeerId; }
    public void setSenderPeerId(String senderPeerId) { this.senderPeerId = senderPeerId; }

    public int getTtl() { return ttl; }
    public void setTtl(int ttl) { this.ttl = ttl; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}