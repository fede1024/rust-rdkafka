//! Example demonstrating how to use the AdminClient with OAUTHBEARER authentication.
//!
//! This example shows how to implement a custom ClientContext that provides
//! OAuth token generation for the AdminClient.

use std::error::Error;

use rdkafka::admin::AdminClient;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::ClientConfig;

/// Custom context that implements OAuth token generation
struct OAuthClientContext;

impl ClientContext for OAuthClientContext {
    // Enable OAuth token refresh
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    /// Generate an OAuth token
    ///
    /// In a real application, this would:
    /// - Call your OAuth provider's token endpoint
    /// - Retrieve a valid access token
    /// - Return the token with appropriate expiration time
    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        // Example token generation (replace with actual OAuth logic)
        println!("Generating OAuth token...");
        
        // In a real implementation, you would:
        // 1. Call your OAuth provider (e.g., Azure AD, Okta, etc.)
        // 2. Get an access token
        // 3. Return it with the appropriate lifetime
        
        Ok(OAuthToken {
            token: "your-oauth-token-here".to_string(),
            principal_name: "your-principal-name".to_string(),
            lifetime_ms: 3600000, // 1 hour in milliseconds
        })
    }
}

fn main() {
    // Configure the admin client with OAUTHBEARER
    let _admin_client: AdminClient<OAuthClientContext> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "OAUTHBEARER")
        // Optional: configure OAuth-specific settings
        // .set("sasl.oauthbearer.config", "your-config-here")
        .create_with_context(OAuthClientContext)
        .expect("Admin client creation failed");

    println!("AdminClient with OAUTHBEARER created successfully!");

    // Example: In a real scenario, you would perform actual admin operations
    // like creating topics, deleting topics, etc.
    println!("Admin client is ready to use with OAUTHBEARER authentication");
    
    // The background polling thread will automatically handle
    // OAUTHBEARER token refresh when needed
}

