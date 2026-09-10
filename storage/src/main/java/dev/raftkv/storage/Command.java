package dev.raftkv.storage;

import dev.raftkv.common.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * write in the form it travels through the replicated log.
 * Raft replicates opaque bytes. It has no idea it is carrying a key-value store, and that is
 * enforced by the build: {raft} depends on {common} alone, so nothing
 * in it can even name this class. What Raft guarantees is that every node ends up with the same
 * bytes in the same order; what those bytes mean is decided here, and by whoever calls
 * {@link #applyTo}.
 *   byte    type            1 = PUT, 2 = DELETE
 *   int     key length
 *   bytes   key
 *   int     value length    PUT only
 *   bytes   value           PUT only
 */
public record Command(Type type, Bytes key, Bytes value) {

    /** The operations the store supports. The numbers are on disk, so they must never change. */
    public enum Type {
        PUT(1),
        DELETE(2);

        private final int code;

        Type(int code) {
            this.code = code;
        }

        static Type fromCode(int code) {
            for (Type type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("unknown command type: " + code);
        }
    }

    public Command {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        // A PUT without a value and a DELETE with one are both meaningless, and both would
        // otherwise fail much later, inside apply, on a node that has already committed them.
        if (type == Type.PUT && value == null) {
            throw new IllegalArgumentException("a PUT needs a value");
        }
        if (type == Type.DELETE && value != null) {
            throw new IllegalArgumentException("a DELETE must not carry a value");
        }
    }

    public static Command put(Bytes key, Bytes value) {
        return new Command(Type.PUT, key, value);
    }

    public static Command delete(Bytes key) {
        return new Command(Type.DELETE, key, null);
    }

    public Bytes encode() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buffer)) {
            out.writeByte(type.code);
            out.writeInt(key.length());
            out.write(key.toByteArray());
            if (type == Type.PUT) {
                out.writeInt(value.length());
                out.write(value.toByteArray());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("encoding to memory cannot fail", e);
        }
        return Bytes.of(buffer.toByteArray());
    }

    // Reads a command back out of a committed log entry.
    public static Command decode(Bytes encoded) {
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(encoded.toByteArray()))) {
            Type type = Type.fromCode(in.readByte());

            byte[] key = new byte[in.readInt()];
            in.readFully(key);
            if (type == Type.DELETE) {
                return delete(Bytes.of(key));
            }

            byte[] value = new byte[in.readInt()];
            in.readFully(value);
            return put(Bytes.of(key), Bytes.of(value));
        } catch (IOException e) {
            throw new IllegalArgumentException("not a valid command: " + encoded.length() + " bytes", e);
        }
    }

    // Carries this command out against a store
    public void applyTo(LsmEngine engine) throws IOException {
        switch (type) {
            case PUT -> engine.put(key, value);
            case DELETE -> engine.delete(key);
        }
    }

    @Override
    public String toString() {
        return type == Type.PUT
                ? "PUT " + key.asString() + "=" + value.asString()
                : "DELETE " + key.asString();
    }
}