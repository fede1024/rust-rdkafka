use anyhow::Context;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::FromClientConfig;
use rdkafka::ClientConfig;

pub async fn create_admin(
    bootstrap_servers: &str,
) -> anyhow::Result<AdminClient<DefaultClientContext>> {
    let mut admin_client_config = ClientConfig::default();
    admin_client_config.set("bootstrap.servers", bootstrap_servers);
    AdminClient::from_config(&admin_client_config).context("error creating admin client")
}
