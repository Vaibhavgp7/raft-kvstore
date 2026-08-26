package dev.raftkv.storage;

public interface KeyHasher {
    record Hash128(long h1, long h2){ }

    Hash128 hash(byte[] key);
}
