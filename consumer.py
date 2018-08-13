import time

from boto import kinesis

from settings import KINESIS_REGION, KINESIS_STREAM_NAME

FLUSH_INTERVAL = 5
BATCH_SIZE = 20
running = True


def process_messages(batch_msgs):
    print('messages processed: {}'.format(len(batch_msgs['Records'])))
    for msg in batch_msgs['Records']:
        print('message: "{}" offset: {}'.format(msg['Data'], msg['SequenceNumber']))


print('Connect to Kinesis Streams')
kinesis = kinesis.connect_to_region(region_name=KINESIS_REGION)
stream = kinesis.describe_stream(KINESIS_STREAM_NAME)
shardId = stream['StreamDescription']['Shards'][0]['ShardId']
shardIterator = kinesis.get_shard_iterator(KINESIS_STREAM_NAME, shardId, 'TRIM_HORIZON')

print('Kinesis consumer started!')
while running:
    msgs = kinesis.get_records(shardIterator['ShardIterator'], limit=BATCH_SIZE)

    process_messages(msgs)
    shardIterator['ShardIterator'] = msgs['NextShardIterator']

    print('\nnext batch in {} seconds...'.format(FLUSH_INTERVAL))
    time.sleep(FLUSH_INTERVAL)
