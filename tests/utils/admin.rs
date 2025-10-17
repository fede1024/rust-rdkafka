use anyhow::{bail, Context};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::FromClientConfig;
use rdkafka::ClientConfig;

pub async fn create_admin_client(
    bootstrap_servers: &str,
) -> anyhow::Result<AdminClient<DefaultClientContext>> {
    let mut admin_client_config = ClientConfig::default();
    admin_client_config.set("bootstrap.servers", bootstrap_servers);
    AdminClient::from_config(&admin_client_config).context("error creating admin client")
}

pub async fn create_topic(
    admin_client: &AdminClient<DefaultClientContext>,
    topic_name: &'_ str,
) -> anyhow::Result<String> {
    let topic_results = admin_client
        .create_topics(&new_topic_vec(&topic_name, None), &AdminOptions::default())
        .await
        .context("error creating topics")?;
    for topic_result in topic_results {
        if topic_result.is_err() {
            bail!("failed to create topic: {:?}", topic_result.unwrap_err());
        };
    }
    Ok(topic_name.to_string())
}

pub fn new_topic_vec(topic_name: &'_ str, num_partitions: Option<i32>) -> Vec<NewTopic<'_>> {
    let new_topic = NewTopic::new(&topic_name, num_partitions.unwrap_or(1), TopicReplication::Fixed(1));
    vec![new_topic]
}
