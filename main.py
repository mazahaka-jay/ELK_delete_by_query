import requests
from elasticsearch import Elasticsearch
import time
import logging

# Set up logging
logging.basicConfig(filename='elasticsearch_delete.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Elasticsearch configuration
ELASTICSEARCH_HOST = '<url>'
ELASTICSEARCH_PORT = 443
ELASTICSEARCH_USERNAME = '<username>'
ELASTICSEARCH_PASSWORD = '<password>'

# Telegram configuration
TELEGRAM_BOT_TOKEN = '<TELEGRAM_BOT_TOKEN>'
TELEGRAM_API_URL = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
TELEGRAM_CHAT_ID = '<TELEGRAM_CHAT_ID>'

# List of indexes to delete events from
INDEXES = ['index1', 'index2', 'index3']


def send_telegram_message(message):
    try:
        requests.post(TELEGRAM_API_URL, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message})
    except Exception as e:
        logging.error(f'Error sending Telegram message: {str(e)}')

def check_elasticsearch_connection():
    try:
        es = Elasticsearch(
            [{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}],
            http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
            scheme="https"  # Specify HTTPS
        )
        if es.ping():
            print("Connected to Elasticsearch.")
            return True
        else:
            print("Could not connect to Elasticsearch.")
            return False
    except ConnectionError as e:
        print(f"Connection Error: {e}")
        return False

def delete_events_by_query(index):
    es = Elasticsearch(
        [{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}],
        http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
        scheme="https"  # Specify HTTPS
    )
    query = {
      "query": {
        "bool": {      
          "must": [],
          "filter": [],
          "should": [],
          "must_not": []
        }
      }
    }
    try:
        response = es.delete_by_query(index=index, body=query, wait_for_completion=False)
        task_id = response['task']
        return task_id
    except Exception as e:
        logging.error(f'Error deleting events from index {index}: {str(e)}')
        send_telegram_message(f'Error deleting events from index {index}: {str(e)}')
        return None

def check_task_status(task_id):
    es = Elasticsearch(
        [{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}],
        http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
        scheme="https"  # Specify HTTPS
    )
    try:
        response = es.tasks.get(task_id=task_id)
        return response['completed']
    except Exception as e:
        logging.error(f'Error checking task status for task {task_id}: {str(e)}')
        send_telegram_message(f'Error checking task status for task {task_id}: {str(e)}')
        return False

def main():

    # Example usage
    if check_elasticsearch_connection():
        # Perform your Elasticsearch operations here
        print("Performing Elasticsearch operations...")
    else:
        print("Cannot proceed with Elasticsearch operations due to connection issues.")


    for index in INDEXES:
        task_id = delete_events_by_query(index)
        if task_id:
            send_telegram_message(f'New task {task_id} for index {index} is created.')
            while not check_task_status(task_id):
                time.sleep(300)  # Check task status every 5 minutes
            send_telegram_message(f'Deletion task for index {index} is completed.')
        else:
            send_telegram_message(f'Error initiating deletion task for index {index}.')
    send_telegram_message(f'All tasks finished')


if __name__ == "__main__":
    main()
