/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.webhooks;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class StandardWebhooksAuthenticatorTest {

    @Test
    public void addAuthorizationHeader() throws URISyntaxException {
        Clock clock = Clock.fixed(Instant.ofEpochSecond(1234), ZoneOffset.UTC);
        UUID messageId = UUID.fromString("22bd292a-71ab-46fe-a460-8632d6754ac6");
        StandardWebhooksAuthenticator authenticator = new StandardWebhooksAuthenticator(
                "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw", clock);

        URI testURI = new URI("http://example.com");
        String testEventContent = "{\"hello\":\"world\"}";
        HttpRequest.Builder builder = HttpRequest.newBuilder(testURI);
        builder.POST(HttpRequest.BodyPublishers.ofString(testEventContent));
        authenticator.setAuthorizationHeader(builder, testEventContent, messageId);
        HttpRequest request = builder.build();

        HttpHeaders headers = request.headers();

        Optional<String> idHeader = headers.firstValue("webhook-id");
        Assertions.assertTrue(idHeader.isPresent());
        Assertions.assertEquals("msg_22bd292a-71ab-46fe-a460-8632d6754ac6", idHeader.get());

        Optional<String> timestampHeader = headers.firstValue("webhook-timestamp");
        Assertions.assertTrue(timestampHeader.isPresent());
        Assertions.assertEquals("1234", timestampHeader.get());

        Optional<String> signatureHeader = headers.firstValue("webhook-signature");
        Assertions.assertTrue(signatureHeader.isPresent());
        String[] sigParts = signatureHeader.get().split(",");
        Assertions.assertEquals(2, sigParts.length);
        Assertions.assertEquals("v1", sigParts[0]);

        // https://www.standardwebhooks.com/verify
        String expected = "v1,qCVBRIv6rKQVhSJBAmUSE9GkdCdPe2j6xzzkm89UcoA=";

        Assertions.assertEquals(expected, signatureHeader.get());
    }
}
