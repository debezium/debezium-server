/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.databricks.zerobus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.Test;

class ZerobusOAuthBearerLoginCallbackHandlerTest {

    private static final String TOKEN_RESPONSE = "{\"access_token\":\"abc.def.ghi\",\"token_type\":\"Bearer\",\"expires_in\":3600}";

    @Test
    void derivesTokenEndpointAndAudienceFromWorkspace() throws Exception {
        RecordingHttpClient http = new RecordingHttpClient(TOKEN_RESPONSE);
        ZerobusOAuthBearerLoginCallbackHandler handler = newHandler(http, Map.of(
                "workspaceUrl", "https://dbc-a1b2.cloud.databricks.com/",
                "workspaceId", "1234567890123456",
                "clientId", "sp-client",
                "clientSecret", "sp-secret",
                "tables", "main.default.customers"));

        OAuthBearerTokenCallback callback = invoke(handler);

        assertThat(callback.token().value()).isEqualTo("abc.def.ghi");
        assertThat(callback.token().principalName()).isEqualTo("sp-client");
        assertThat(http.lastUrl).isEqualTo("https://dbc-a1b2.cloud.databricks.com/oidc/v1/token");
        assertThat(http.lastBody).contains("grant_type=client_credentials");
        assertThat(http.lastBody).contains("scope=all-apis");
        assertThat(http.lastBody).contains("api%3A%2F%2Fdatabricks%2Fworkspaces%2F1234567890123456%2FzerobusDirectWriteApi");
        assertThat(http.lastBody).contains("authorization_details=");
        assertThat(http.lastAuthHeader).startsWith("Basic ");
    }

    @Test
    void isJsonObjectAcceptsOnlyObjects() {
        assertThat(ZerobusChangeConsumer.isJsonObject("{\"id\":1}")).isTrue();
        assertThat(ZerobusChangeConsumer.isJsonObject("  { \"id\":1 } ")).isTrue();
        assertThat(ZerobusChangeConsumer.isJsonObject(null)).isFalse();
        assertThat(ZerobusChangeConsumer.isJsonObject("null")).isFalse();
        assertThat(ZerobusChangeConsumer.isJsonObject("[1,2]")).isFalse();
        assertThat(ZerobusChangeConsumer.isJsonObject("")).isFalse();
    }

    @Test
    void isQualifiedTableRequiresThreeParts() {
        assertThat(ZerobusChangeConsumer.isQualifiedTable("cat.sch.tbl")).isTrue();
        assertThat(ZerobusChangeConsumer.isQualifiedTable("mysqldbz")).isFalse();
        assertThat(ZerobusChangeConsumer.isQualifiedTable("cat.sch")).isFalse();
        assertThat(ZerobusChangeConsumer.isQualifiedTable("cat..tbl")).isFalse();
        assertThat(ZerobusChangeConsumer.isQualifiedTable(null)).isFalse();
    }

    @Test
    void buildsAuthorizationDetailsForTable() {
        String authz = ZerobusOAuthBearerLoginCallbackHandler.buildAuthorizationDetails("cat.sch.tbl");
        assertThat(authz).contains("\"privileges\":[\"USE CATALOG\"]").contains("\"object_full_path\":\"cat\"");
        assertThat(authz).contains("\"privileges\":[\"USE SCHEMA\"]").contains("\"object_full_path\":\"cat.sch\"");
        assertThat(authz).contains("\"privileges\":[\"SELECT\",\"MODIFY\"]").contains("\"object_full_path\":\"cat.sch.tbl\"");
    }

    @Test
    void allowsExplicitEndpointAndAudienceOverride() throws Exception {
        RecordingHttpClient http = new RecordingHttpClient(TOKEN_RESPONSE);
        ZerobusOAuthBearerLoginCallbackHandler handler = newHandler(http, Map.of(
                "tokenEndpoint", "https://custom/oidc/v1/token",
                "audience", "custom-audience",
                "clientId", "sp-client",
                "clientSecret", "sp-secret",
                "tables", "main.default.customers"));

        invoke(handler);

        assertThat(http.lastUrl).isEqualTo("https://custom/oidc/v1/token");
        assertThat(http.lastBody).contains("resource=custom-audience");
    }

    @Test
    void cachesTokenUntilNearExpiry() throws Exception {
        RecordingHttpClient http = new RecordingHttpClient(TOKEN_RESPONSE);
        ZerobusOAuthBearerLoginCallbackHandler handler = newHandler(http, baseOptions());

        invoke(handler);
        invoke(handler);

        assertThat(http.callCount).isEqualTo(1);
    }

    @Test
    void usesStaticTokenDirectlyWithoutExchange() throws Exception {
        RecordingHttpClient http = new RecordingHttpClient(TOKEN_RESPONSE);
        ZerobusOAuthBearerLoginCallbackHandler handler = newHandler(http, Map.of(
                "token", "dapiPAT123",
                "clientId", "sp-client"));

        OAuthBearerTokenCallback callback = invoke(handler);

        assertThat(callback.token().value()).isEqualTo("dapiPAT123");
        assertThat(http.callCount).isZero();
    }

    @Test
    void failsWhenRequiredOptionMissing() {
        ZerobusOAuthBearerLoginCallbackHandler handler = new ZerobusOAuthBearerLoginCallbackHandler();
        Map<String, Object> options = new HashMap<>(baseOptions());
        options.remove("clientSecret");

        assertThatThrownBy(() -> handler.configure(Map.of(), "OAUTHBEARER", jaas(options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clientSecret");
    }

    @Test
    void rejectsNonOAuthBearerMechanism() {
        ZerobusOAuthBearerLoginCallbackHandler handler = new ZerobusOAuthBearerLoginCallbackHandler();

        assertThatThrownBy(() -> handler.configure(Map.of(), "PLAIN", jaas(baseOptions())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("OAUTHBEARER");
    }

    // ---- helpers -------------------------------------------------------------------------------

    private static Map<String, Object> baseOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("workspaceUrl", "https://dbc-a1b2.cloud.databricks.com");
        options.put("workspaceId", "1234567890123456");
        options.put("clientId", "sp-client");
        options.put("clientSecret", "sp-secret");
        options.put("tables", "main.default.customers");
        return options;
    }

    private static ZerobusOAuthBearerLoginCallbackHandler newHandler(RecordingHttpClient http, Map<String, ?> options) {
        ZerobusOAuthBearerLoginCallbackHandler handler = new ZerobusOAuthBearerLoginCallbackHandler();
        handler.setHttpClient(http);
        handler.configure(Map.of(), "OAUTHBEARER", jaas(options));
        return handler;
    }

    private static List<AppConfigurationEntry> jaas(Map<String, ?> options) {
        List<AppConfigurationEntry> entries = new ArrayList<>();
        entries.add(new AppConfigurationEntry(
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                options));
        return entries;
    }

    private static OAuthBearerTokenCallback invoke(ZerobusOAuthBearerLoginCallbackHandler handler) throws Exception {
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[]{ callback });
        return callback;
    }

    private static final class RecordingHttpClient implements TokenHttpClient {
        private final String response;
        int callCount;
        String lastUrl;
        String lastAuthHeader;
        String lastBody;

        RecordingHttpClient(String response) {
            this.response = response;
        }

        @Override
        public String postForm(String url, String authorizationHeader, String formBody) throws IOException {
            callCount++;
            lastUrl = url;
            lastAuthHeader = authorizationHeader;
            lastBody = formBody;
            return response;
        }
    }
}
