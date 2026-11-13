package modell;

import java.util.Objects;

public class ByteRange {
    private final long offset;
    private final long length;

    public ByteRange(long offset, long length) {
        this.offset = offset;
        this.length = length;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteRange that)) return false;
        return offset == that.offset && length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, length);
    }

    @Override
    public String toString() {
        return "ByteRange{" +
                "offset=" + offset +
                ", length=" + length +
                '}';
    }
}
