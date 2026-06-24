import com.fasterxml.jackson.jr.ob.JSON;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.keycloak.authorization.client.AuthzClient;
import org.keycloak.authorization.client.Configuration;
import org.keycloak.authorization.client.util.Http;
import org.keycloak.representations.AccessTokenResponse;
import org.neo4j.connectors.authn.AuthenticationToken;

public class KeycloakOIDCAuthenticationSupplier implements Supplier<AuthenticationToken> {
    private final String username;
    private final String password;
    private final Configuration config;
    private final AuthzClient client;
    private final Http http;
    private final String url;
    private final AtomicReference<AuthenticationTokenAndTime> token = new AtomicReference<>();

    KeycloakOIDCAuthenticationSupplier(String username, String password, Configuration config) {
        this.username = username;
        this.password = password;
        this.config = config;
        this.client = AuthzClient.create(config);
        this.url = constructUrl(config);
        this.http = new Http(config, config.getClientCredentialsProvider());
    }

    public static Supplier<AuthenticationToken> of(String username, String password, Configuration config) {
        return new KeycloakOIDCAuthenticationSupplier(username, password, config);
    }

    private String constructUrl(Configuration config) {
        return config.getAuthServerUrl() + "/realms/" + config.getRealm() + "/protocol/openid-connect/token";
    }

    public boolean currentTokenIsExpired() {
        return token.get() == null || token.get().expireAt.isBefore(Instant.now());
    }

    @Override
    public AuthenticationToken get() {
        AuthenticationTokenAndTime freshToken = this.token.updateAndGet(this::get0);
        return freshToken.toAuthenticationToken();
    }

    private AuthenticationTokenAndTime get0(AuthenticationTokenAndTime previous) {
        if (previous == null) {
            return fetch();
        } else {
            return refresh(previous.refreshToken);
        }
    }

    private AuthenticationTokenAndTime fetch() {
        try {
            AccessTokenResponse response = this.client.obtainAccessToken(this.username, this.password);
            return AuthenticationTokenAndTime.of(response);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private AuthenticationTokenAndTime refresh(String refreshToken) {
        try {
            AccessTokenResponse response = this.http
                    .<AccessTokenResponse>post(this.url)
                    .authentication()
                    .client()
                    .form()
                    .param("grant_type", "refresh_token")
                    .param("refresh_token", refreshToken)
                    .param("client_id", this.config.getResource())
                    .param("client_secret", (String)
                            this.config.getCredentials().get("secret"))
                    .response()
                    .json(AccessTokenResponse.class)
                    .execute();
            return AuthenticationTokenAndTime.of(response);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final class AuthenticationTokenAndTime {
        static final Base64.Decoder DECODER = Base64.getDecoder();
        private final String token;
        private final Instant expireAt;
        private final String refreshToken;

        public AuthenticationTokenAndTime(String token, Instant expireAt, String refreshToken) {
            this.token = token;
            this.expireAt = expireAt;
            this.refreshToken = refreshToken;
        }

        static AuthenticationTokenAndTime of(AccessTokenResponse accessTokenResponse) throws IOException {
            String token = accessTokenResponse.getToken();
            String[] chunks = token.split("\\.");
            Map<String, Object> payload = JSON.std.mapFrom(DECODER.decode(chunks[1]));
            long epoch = ((Number) payload.get("exp")).longValue();
            Instant expireAt = Instant.ofEpochSecond(epoch);

            return new AuthenticationTokenAndTime(token, expireAt, accessTokenResponse.getRefreshToken());
        }

        AuthenticationToken toAuthenticationToken() {
            return AuthenticationToken.bearer(token, expireAt);
        }
    }
}