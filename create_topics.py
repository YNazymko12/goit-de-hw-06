from colorama import Fore, Style, init
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Ініціалізація colorama
init(autoreset=True)

# Створення клієнта Kafka
try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
    print(Fore.GREEN + "✅ KafkaAdminClient successfully initialized.\n")
except Exception as e:
    print(Fore.RED + f"❌ Failed to initialize KafkaAdminClient: {e}")
    exit()

# Визначення імен топіків
my_name = "yuliia"
topic_name_in = f"{my_name}_iot_sensors_data"
alerts_topic_name = f"{my_name}_iot_alerts"
num_partitions = 2
replication_factor = 1

# Перевірка існування топіків
existing_topics = admin_client.list_topics()
if topic_name_in in existing_topics and alerts_topic_name in existing_topics:
    print(f"{Fore.YELLOW}⚠️ Topics '{topic_name_in}' and '{alerts_topic_name}' already exist.")
else:
    # Створення топіків
    new_topics = [
        NewTopic(name=topic_name_in, num_partitions=num_partitions, replication_factor=replication_factor),
        NewTopic(name=alerts_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    ]

    try:
        print(f"{Fore.CYAN}Creating topics: {topic_name_in} and {alerts_topic_name}...")
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print(f"✅ {Fore.GREEN}Topics '{topic_name_in}' and '{alerts_topic_name}' created successfully.")
    except Exception as e:
        print(f"{Fore.RED}An error occurred while creating topics: {e}")

# Закриття клієнта
admin_client.close()
print(Fore.GREEN + "\n🔒 KafkaAdminClient connection closed.")
