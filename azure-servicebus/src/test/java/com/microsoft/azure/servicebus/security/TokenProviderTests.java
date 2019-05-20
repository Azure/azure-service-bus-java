package com.microsoft.azure.servicebus.security;

import static org.junit.Assert.*;

import java.time.Instant;
import org.junit.Test;

public class TokenProviderTests {
	private static final SecurityToken TEST_TOKEN = new SecurityToken(SecurityTokenType.JWT, "testAudience", "tokenString", Instant.now(), Instant.MAX);
	
	@Test
	public void aadCallbackTokenProviderTest() {
		TokenProvider tokenProvider = TokenProvider.createAzureActiveDirectoryTokenProvider((audience, authority) -> {
			return TEST_TOKEN;
		}, null);
		
		assertEquals(TEST_TOKEN, tokenProvider.getSecurityTokenAsync("testAudience").join());
	}
}
