package com.example.demo.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;

/**
 * Algorithmic Forward Error Correction (FEC) & Sub-Block Integrity Recovery Engine.
 *
 * Differentiates from conventional P2P protocols (e.g., standard BitTorrent whole-piece discard) by combining:
 * 1. 64 KB Sub-Block Merkle CRC32C Checksumming: Isolates byte corruption to exact 64 KB sub-blocks inside a 4 MB window.
 * 2. Systematic XOR Parity Stripe Reconstruction (RAID-5 style O(N) single-erasure recovery).
 * 3. Galois Field GF(2^8) Reed-Solomon Vandermonde Matrix Erasure Coding (polynomial x^8 + x^4 + x^3 + x^2 + 1 = 0x11D)
 *    capable of reconstructing up to M simultaneous lost or corrupted blocks across K data blocks without re-requesting
 *    original blocks from dead seeders.
 */
@Service
public class ErasureCodingRecoveryEngine {

    public static final int SUB_BLOCK_SIZE = 65536; // 64 KB sub-block granularity
    private static final int GF_PRIMITIVE_POLY = 0x11D; // x^8 + x^4 + x^3 + x^2 + 1

    private static final int[] GF_EXP = new int[512];
    private static final int[] GF_LOG = new int[256];

    static {
        int x = 1;
        for (int i = 0; i < 255; i++) {
            GF_EXP[i] = x;
            GF_LOG[x] = i;
            x <<= 1;
            if ((x & 0x100) != 0) {
                x ^= GF_PRIMITIVE_POLY;
            }
        }
        for (int i = 255; i < 512; i++) {
            GF_EXP[i] = GF_EXP[i - 255];
        }
    }

    private final AtomicLong totalFecRecoveries = new AtomicLong(0);
    private final AtomicLong subBlocksVerified = new AtomicLong(0);

    public long getTotalFecRecoveries() {
        return totalFecRecoveries.get();
    }

    public long getSubBlocksVerified() {
        return subBlocksVerified.get();
    }

    // =========================================================================
    // 1. Hardware-Accelerated CRC32C Sub-Block Merkle Verification
    // =========================================================================

    /**
     * Computes whole-window CRC32C and 64 KB sub-block CRC32C digests directly from a FileChannel range.
     */
    public WindowIntegrityMetadata computeWindowMetadata(FileChannel channel, long offset, long length) throws IOException {
        CRC32C windowCrc = new CRC32C();
        List<Long> subBlockCrcs = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.allocateDirect(SUB_BLOCK_SIZE);

        long remaining = length;
        long currentOffset = offset;

        while (remaining > 0) {
            int toRead = (int) Math.min(SUB_BLOCK_SIZE, remaining);
            buf.clear();
            buf.limit(toRead);

            int readTotal = 0;
            while (buf.hasRemaining()) {
                int r = channel.read(buf, currentOffset + readTotal);
                if (r <= 0) break;
                readTotal += r;
            }
            if (readTotal <= 0) break;

            buf.flip();
            CRC32C subCrc = new CRC32C();
            subCrc.update(buf);
            long subDigest = subCrc.getValue();
            subBlockCrcs.add(subDigest);

            buf.rewind();
            windowCrc.update(buf);

            currentOffset += readTotal;
            remaining -= readTotal;
            subBlocksVerified.incrementAndGet();
        }

        return new WindowIntegrityMetadata(windowCrc.getValue(), subBlockCrcs);
    }

    /**
     * Pinpoints exact 64 KB sub-block indices within a streamed window that failed CRC32C verification.
     * Instead of discarding an entire 4 MB window on a transient bit error, only the corrupted 64 KB sub-block
     * needs to be re-fetched or reconstructed via FEC parity.
     */
    public List<Integer> findCorruptedSubBlocks(FileChannel channel, long windowOffset, long windowLength, List<Long> expectedSubBlockCrcs) throws IOException {
        List<Integer> corruptedIndices = new ArrayList<>();
        if (expectedSubBlockCrcs == null || expectedSubBlockCrcs.isEmpty()) {
            return corruptedIndices;
        }

        ByteBuffer buf = ByteBuffer.allocateDirect(SUB_BLOCK_SIZE);
        long remaining = windowLength;
        long currentOffset = windowOffset;
        int blockIdx = 0;

        while (remaining > 0 && blockIdx < expectedSubBlockCrcs.size()) {
            int toRead = (int) Math.min(SUB_BLOCK_SIZE, remaining);
            buf.clear();
            buf.limit(toRead);

            int readTotal = 0;
            while (buf.hasRemaining()) {
                int r = channel.read(buf, currentOffset + readTotal);
                if (r <= 0) break;
                readTotal += r;
            }

            buf.flip();
            CRC32C subCrc = new CRC32C();
            subCrc.update(buf);
            long actualCrc = subCrc.getValue();

            if (actualCrc != expectedSubBlockCrcs.get(blockIdx)) {
                corruptedIndices.add(blockIdx);
            }

            currentOffset += toRead;
            remaining -= toRead;
            blockIdx++;
        }

        return corruptedIndices;
    }

    // =========================================================================
    // 2. Systematic XOR Parity Stripe Recovery (O(N) Single-Erasure Recovery)
    // =========================================================================

    /**
     * Computes the XOR parity block P = D_0 ^ D_1 ^ ... ^ D_{k-1} across a stripe of data blocks.
     */
    public byte[] computeXorParityBlock(List<byte[]> dataBlocks, int blockSize) {
        byte[] parity = new byte[blockSize];
        for (byte[] block : dataBlocks) {
            if (block == null) continue;
            int limit = Math.min(block.length, blockSize);
            for (int i = 0; i < limit; i++) {
                parity[i] ^= block[i];
            }
        }
        return parity;
    }

    /**
     * Reconstructs a single lost or corrupted data block D_missing from the remaining k-1 data blocks and parity block P:
     * D_missing = P ^ (XOR of all present D_i).
     */
    public byte[] reconstructSingleErasureXor(List<byte[]> stripeBlocksWithOneNull, byte[] parityBlock, int blockSize) {
        byte[] recovered = Arrays.copyOf(parityBlock, blockSize);
        for (byte[] block : stripeBlocksWithOneNull) {
            if (block == null) continue;
            int limit = Math.min(block.length, blockSize);
            for (int i = 0; i < limit; i++) {
                recovered[i] ^= block[i];
            }
        }
        totalFecRecoveries.incrementAndGet();
        return recovered;
    }

    // =========================================================================
    // 3. Galois Field GF(2^8) Reed-Solomon Multi-Block Erasure Coding
    // =========================================================================

    public static int gfAdd(int a, int b) {
        return (a ^ b) & 0xFF;
    }

    public static int gfMul(int a, int b) {
        if (a == 0 || b == 0) return 0;
        return GF_EXP[GF_LOG[a & 0xFF] + GF_LOG[b & 0xFF]];
    }

    public static int gfDiv(int a, int b) {
        if (b == 0) throw new ArithmeticException("GF(2^8) division by zero");
        if (a == 0) return 0;
        return GF_EXP[(GF_LOG[a & 0xFF] - GF_LOG[b & 0xFF] + 255) % 255];
    }

    public static int gfPow(int a, int n) {
        if (n == 0) return 1;
        if (a == 0) return 0;
        return GF_EXP[(GF_LOG[a & 0xFF] * n) % 255];
    }

    /**
     * Generates `parityShards` Reed-Solomon parity blocks from `dataShards` equal-length data blocks
     * over GF(2^8) using a Cauchy/Vandermonde generator matrix.
     */
    public byte[][] encodeReedSolomonParity(byte[][] dataBlocks, int parityShards) {
        int k = dataBlocks.length;
        int blockSize = dataBlocks[0].length;
        byte[][] parityBlocks = new byte[parityShards][blockSize];

        for (int p = 0; p < parityShards; p++) {
            for (int d = 0; d < k; d++) {
                // Vandermonde coefficient alpha^(p * d) where alpha = d + 1
                int coeff = gfPow(d + 1, p);
                byte[] src = dataBlocks[d];
                byte[] dst = parityBlocks[p];
                if (coeff == 1) {
                    for (int b = 0; b < blockSize; b++) {
                        dst[b] ^= src[b];
                    }
                } else {
                    for (int b = 0; b < blockSize; b++) {
                        dst[b] ^= (byte) gfMul(coeff, src[b] & 0xFF);
                    }
                }
            }
        }
        return parityBlocks;
    }

    /**
     * Reconstructs up to `parityShards` simultaneous missing data blocks using Gaussian elimination
     * in GF(2^8) over the surviving data and parity shards.
     *
     * @param dataBlocks   Array of k data blocks (missing blocks are null)
     * @param parityBlocks Array of m parity blocks (available parity blocks are non-null)
     * @param blockSize    Byte length of each block
     * @return Reconstructed full dataBlocks array with all nulls repaired in-place
     */
    public byte[][] reconstructReedSolomonErasures(byte[][] dataBlocks, byte[][] parityBlocks, int blockSize) {
        int k = dataBlocks.length;
        int m = parityBlocks.length;

        List<Integer> missingDataIndices = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            if (dataBlocks[i] == null) {
                missingDataIndices.add(i);
            }
        }

        int numMissing = missingDataIndices.size();
        if (numMissing == 0) {
            return dataBlocks;
        }
        if (numMissing > m) {
            throw new IllegalArgumentException("Cannot recover " + numMissing + " missing blocks with only " + m + " parity blocks");
        }

        // Fast path for single erasure using Parity 0 (which has Vandermonde row p=0 => all coefficients = 1, i.e., XOR parity)
        if (numMissing == 1 && parityBlocks[0] != null) {
            int missingIdx = missingDataIndices.get(0);
            byte[] recovered = Arrays.copyOf(parityBlocks[0], blockSize);
            for (int d = 0; d < k; d++) {
                if (d != missingIdx && dataBlocks[d] != null) {
                    for (int b = 0; b < blockSize; b++) {
                        recovered[b] ^= dataBlocks[d][b];
                    }
                }
            }
            dataBlocks[missingIdx] = recovered;
            totalFecRecoveries.incrementAndGet();
            return dataBlocks;
        }

        // General multi-erasure recovery in GF(2^8):
        // Select `numMissing` available parity rows
        int[] selectedParityRows = new int[numMissing];
        int found = 0;
        for (int p = 0; p < m && found < numMissing; p++) {
            if (parityBlocks[p] != null) {
                selectedParityRows[found++] = p;
            }
        }
        if (found < numMissing) {
            throw new IllegalStateException("Not enough surviving parity blocks to recover " + numMissing + " erasures");
        }

        // Build submatrix A (numMissing x numMissing) for the missing columns
        // and RHS byte vectors B (numMissing x blockSize) = Parity_p ^ sum_{present d} coeff(p,d) * Data_d
        int[][] matrix = new int[numMissing][numMissing];
        byte[][] rhs = new byte[numMissing][blockSize];

        for (int r = 0; r < numMissing; r++) {
            int pRow = selectedParityRows[r];
            System.arraycopy(parityBlocks[pRow], 0, rhs[r], 0, blockSize);

            // Subtract (XOR) contribution of known data blocks from RHS
            for (int d = 0; d < k; d++) {
                if (dataBlocks[d] != null) {
                    int coeff = gfPow(d + 1, pRow);
                    byte[] known = dataBlocks[d];
                    for (int b = 0; b < blockSize; b++) {
                        rhs[r][b] ^= (byte) gfMul(coeff, known[b] & 0xFF);
                    }
                }
            }

            // Populate unknown columns in matrix
            for (int c = 0; c < numMissing; c++) {
                int missingCol = missingDataIndices.get(c);
                matrix[r][c] = gfPow(missingCol + 1, pRow);
            }
        }

        // Invert `matrix` in GF(2^8) using Gauss-Jordan elimination and solve for missing blocks
        int[][] inv = invertGfMatrix(matrix, numMissing);

        for (int c = 0; c < numMissing; c++) {
            int missingIdx = missingDataIndices.get(c);
            byte[] recovered = new byte[blockSize];
            for (int r = 0; r < numMissing; r++) {
                int factor = inv[c][r];
                if (factor == 0) continue;
                byte[] rhsRow = rhs[r];
                for (int b = 0; b < blockSize; b++) {
                    recovered[b] ^= (byte) gfMul(factor, rhsRow[b] & 0xFF);
                }
            }
            dataBlocks[missingIdx] = recovered;
            totalFecRecoveries.incrementAndGet();
        }

        return dataBlocks;
    }

    private int[][] invertGfMatrix(int[][] matrix, int n) {
        int[][] aug = new int[n][2 * n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(matrix[i], 0, aug[i], 0, n);
            aug[i][n + i] = 1;
        }

        for (int col = 0; col < n; col++) {
            int pivot = col;
            while (pivot < n && aug[pivot][col] == 0) {
                pivot++;
            }
            if (pivot == n) {
                throw new IllegalStateException("Singular matrix in GF(2^8) Reed-Solomon decoder");
            }
            if (pivot != col) {
                int[] tmp = aug[col];
                aug[col] = aug[pivot];
                aug[pivot] = tmp;
            }

            int diag = aug[col][col];
            for (int j = 0; j < 2 * n; j++) {
                aug[col][j] = gfDiv(aug[col][j], diag);
            }

            for (int row = 0; row < n; row++) {
                if (row != col && aug[row][col] != 0) {
                    int factor = aug[row][col];
                    for (int j = 0; j < 2 * n; j++) {
                        aug[row][j] = gfAdd(aug[row][j], gfMul(factor, aug[col][j]));
                    }
                }
            }
        }

        int[][] inv = new int[n][n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(aug[i], n, inv[i], 0, n);
        }
        return inv;
    }

    public static class WindowIntegrityMetadata {
        private final long windowCrc32c;
        private final List<Long> subBlockCrcs;

        public WindowIntegrityMetadata(long windowCrc32c, List<Long> subBlockCrcs) {
            this.windowCrc32c = windowCrc32c;
            this.subBlockCrcs = subBlockCrcs;
        }

        public long getWindowCrc32c() { return windowCrc32c; }
        public List<Long> getSubBlockCrcs() { return subBlockCrcs; }
    }
}
