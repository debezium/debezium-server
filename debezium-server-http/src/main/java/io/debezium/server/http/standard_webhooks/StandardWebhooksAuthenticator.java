/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.standard_webhooks;

import java.net.http.HttpRequest.Builder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.server.http.Authenticator;

public class StandardWebhooksAuthenticator implements Authenticator {
    static final String SECRET_PREFIX = "whsec_";
    static final String UNBRANDED_MSG_ID_KEY = "webhook-id";
    static final String UNBRANDED_MSG_SIGNATURE_KEY = "webhook-signature";
    static final String UNBRANDED_MSG_TIMESTAMP_KEY = "webhook-timestamp";
    private static final String HMAC_SHA256 = "HmacSHA256";

    private final byte[] key;

    public StandardWebhooksAuthenticator(final String secret) {
        super();

        String sec = secret;
        if (sec.startsWith(StandardWebhooksAuthenticator.SECRET_PREFIX)) {
            sec = sec.substring(StandardWebhooksAuthenticator.SECRET_PREFIX.length());
        }

        byte[] key = Base64.getDecoder().decode(sec);

        // ensure signing secret is between 24 bytes (192 bits) and 64 bytes (512 bits)
        if (key.length < 24 || key.length > 64) {
            throw new DebeziumException("Webhook secret must be between 24 and 64 bytes");
        }

        this.key = key;
    }

    @Override
    public void setAuthorizationHeader(Builder httpRequestBuilder, final String bodyContent, final UUID messageId) {
        final long timestamp = Instant.now().getEpochSecond();
        final String msgId = "msg_" + messageId;
        final String signature = sign(msgId, timestamp, bodyContent);
        httpRequestBuilder.header(StandardWebhooksAuthenticator.UNBRANDED_MSG_ID_KEY, msgId);
        httpRequestBuilder.header(StandardWebhooksAuthenticator.UNBRANDED_MSG_SIGNATURE_KEY, signature);
        httpRequestBuilder.header(StandardWebhooksAuthenticator.UNBRANDED_MSG_TIMESTAMP_KEY, Long.toString(timestamp));
    }

    @Override
    public boolean authenticate() throws InterruptedException {
        return true;
    }

    @VisibleForTesting
    String sign(final String msgId, final long timestamp, final String payload) {
        try {
            String toSign = String.format("%s.%s.%s", msgId, timestamp, payload);
            Mac sha512Hmac = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec keySpec = new SecretKeySpec(this.key, HMAC_SHA256);
            sha512Hmac.init(keySpec);
            byte[] macData = sha512Hmac.doFinal(toSign.getBytes(StandardCharsets.UTF_8));
            String signature = Base64.getEncoder().encodeToString(macData);
            return String.format("v1,%s", signature);
        }
        catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new DebeziumException(e.getMessage());
        }
    }
}