package dev.globalbroadcast.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public record BroadcastPayload(String playerName, String message) {
    public byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeUTF(playerName);
                output.writeUTF(message);
            }
            return bytes.toByteArray();
        } catch (IOException exception) {
            throw new IllegalStateException("Could not encode broadcast payload", exception);
        }
    }

    public static BroadcastPayload decode(byte[] bytes) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            BroadcastPayload payload = new BroadcastPayload(input.readUTF(), input.readUTF());
            if (input.available() != 0) {
                throw new IOException("Unexpected trailing data");
            }
            return payload;
        }
    }

}
