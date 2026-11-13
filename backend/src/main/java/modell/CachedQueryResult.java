package modell;

import java.util.List;

public class CachedQueryResult {
    private final String query;
    private final List<SearchResult> results;
    private final long createdAtMs;
    private volatile long lastHitAtMs;
    private volatile long hitCount;

    public CachedQueryResult(String query, List<SearchResult> results) {
        this.query = query;
        this.results = results;
        this.createdAtMs = System.currentTimeMillis();
        this.lastHitAtMs = this.createdAtMs;
        this.hitCount = 0;
    }

    public void recordHit() {
        this.hitCount++;
        this.lastHitAtMs = System.currentTimeMillis();
    }

    public boolean isExpired(long ttlMs) {
        return (System.currentTimeMillis() - this.lastHitAtMs) > ttlMs;
    }

    public String getQuery() { return query; }
    public List<SearchResult> getResults() { return results; }
    public long getCreatedAtMs() { return createdAtMs; }
    public long getLastHitAtMs() { return lastHitAtMs; }
    public long getHitCount() { return hitCount; }
}
