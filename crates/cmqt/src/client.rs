//! Subscribe helper for MQTT 5 clients (feature `client`).

use rumqttc::v5::{AsyncClient, ClientError, mqttbytes::QoS};

use crate::topic;

/// Subscribe to everything a screen view needs: the screen's topics and
/// `_online`. Retained messages arrive right after the subscription.
pub async fn subscribe_screen(client: &AsyncClient, screen: &str) -> Result<(), ClientError> {
    client.subscribe(topic::screen_filter(screen), QoS::AtLeastOnce).await?;
    client.subscribe(topic::ONLINE, QoS::AtLeastOnce).await
}
