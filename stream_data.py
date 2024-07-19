import boto3
import csv
import time
import argparse
import json


def define_arguments():
    parser = argparse.ArgumentParser(description="Send CSV data to Kinesis Data Streams")
    parser.add_argument("--stream_name", "-sn", required=True, help="Name of the Kinesis Data Stream")
    parser.add_argument("--interval", "-i", type=int, required=True, help="Time interval (in seconds) between two writes")
    args = parser.parse_args()

    return args


def determine_partition_key(species):
    partition_key_mapping = {
        'Iris-setosa': '1',
        'Iris-versicolor': '2',
        'Iris-virginica': '3'
    }
    return partition_key_mapping.get(species, 'unknown')


def send_csv_to_kinesis(stream_name, interval):
    json_file = 'data/iris.json'
    client = boto3.client('kinesis', region_name='us-east-1')

    json_data = json.load(open("data\sensor_data.json"))

    for record in json_data:
        partition_key = determine_partition_key(record['class'])
        response = client.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=partition_key
        )
        print((f"Record sent: {response['SequenceNumber']}"))

        time.sleep(interval)

    


if __name__ == "__main__":
    args = define_arguments()

    send_csv_to_kinesis(args.stream_name, args.interval)