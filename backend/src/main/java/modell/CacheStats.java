package modell;

import java.util.Map;

public class CacheStats {
    private long queryCacheHits;
    private long queryCacheMisses;
    private double hitRate;
    private int cachedQueryCount;
    private int activeSwarmCount;
    private int knownPeersCount;
    private double averageRttMs;
    private Map<String, Integer> swarms;

    public CacheStats() {}

    public CacheStats(long queryCacheHits, long queryCacheMisses, double hitRate,
                      int cachedQueryCount, int activeSwarmCount, int knownPeersCount,
                      double averageRttMs, Map<String, Integer> swarms) {
        this.queryCacheHits = queryCacheHits;
        this.queryCacheMisses = queryCacheMisses;
        this.hitRate = hitRate;
        this.cachedQueryCount = cachedQueryCount;
        this.activeSwarmCount = activeSwarmCount;
        this.knownPeersCount = knownPeersCount;
        this.averageRttMs = averageRttMs;
        this.swarms = swarms;
    }

    public long getQueryCacheHits() { return queryCacheHits; }
    public void setQueryCacheHits(long queryCacheHits) { this.queryCacheHits = queryCacheHits; }

    public long getQueryCacheMisses() { return queryCacheMisses; }
    public void setQueryCacheMisses(long queryCacheMisses) { this.queryCacheMisses = queryCacheMisses; }

    public double getHitRate() { return hitRate; }
    public void setHitRate(double hitRate) { this.hitRate = hitRate; }

    public int getCachedQueryCount() { return cachedQueryCount; }
    public void setCachedQueryCount(int cachedQueryCount) { this.cachedQueryCount = cachedQueryCount; }

    public int getActiveSwarmCount() { return activeSwarmCount; }
    public void setActiveSwarmCount(int activeSwarmCount) { this.activeSwarmCount = activeSwarmCount; }

    public int getKnownPeersCount() { return knownPeersCount; }
    public void setKnownPeersCount(int knownPeersCount) { this.knownPeersCount = knownPeersCount; }

    public double getAverageRttMs() { return averageRttMs; }
    public void setAverageRttMs(double averageRttMs) { this.averageRttMs = averageRttMs; }

    public Map<String, Integer> getSwarms() { return swarms; }
    public void setSwarms(Map<String, Integer> swarms) { this.swarms = swarms; }
}
