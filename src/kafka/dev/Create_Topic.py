from confluent_kafka.admin import AdminClient, NewTopic

conf = {
    'bootstrap.servers': 'localhost:9092'
}

admin_client = AdminClient(conf)

# Define the topic to be created
new_topic = NewTopic("quickstart-events", num_partitions=1, replication_factor=1)

# Create the topic
admin_client.create_topics([new_topic])

# Close the admin client
admin_client.close()
