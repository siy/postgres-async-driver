package com.github.pgasync.io.frontend;

import com.github.pgasync.io.IO;
import com.github.pgasync.message.frontend.SASLInitialResponse;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public class SASLInitialResponseEncoder extends SkipableEncoder<SASLInitialResponse> {

    private static String saslPrep(String value){
        // TODO: Implement me
        return value;
    }

    @Override
    protected byte getMessageId() {
        return 'p';
    }

    @Override
    protected void writeBody(SASLInitialResponse msg, ByteBuffer buffer, Charset encoding) {
        IO.putCString(buffer, msg.getSaslMechanism(), encoding);
        StringBuilder clientFirstMessage = new StringBuilder();
        if (msg.getChannelBindingType() != null && !msg.getChannelBindingType().isBlank()) {
            clientFirstMessage.append("p=").append(msg.getChannelBindingType()).append(",");
        } else {
            clientFirstMessage.append("n,");
        }
        clientFirstMessage.append(",");// It could be 'clientFirstMessage.append("a=").append(msg.getUsername()).append(",");' but it is not needed unless we are using impersonate techniques.
        clientFirstMessage.append("n=,"); // Postgres expects the username here to be an empty string because of the startup message.
        clientFirstMessage.append("r=").append(msg.getNonce());
        byte[] clientFirstMessageContent = clientFirstMessage.toString().getBytes(encoding); // RFC dictates us to use UTF-8 here
        buffer.putInt(clientFirstMessageContent.length);
        buffer.put(clientFirstMessageContent);
    }

    @Override
    public Class<SASLInitialResponse> getMessageType() {
        return SASLInitialResponse.class;
    }
}
