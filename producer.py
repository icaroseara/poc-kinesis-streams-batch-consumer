from boto import kinesis

from settings import KINESIS_PARTITION_KEY, KINESIS_STREAM_NAME, KINESIS_REGION

MESSAGE_COUNT = 1000


print('Kinesis stream producer started!')
kinesis = kinesis.connect_to_region(KINESIS_REGION)

for i in range(MESSAGE_COUNT):
    message = 'hello world - {}'.format(i + 1)
    kinesis.put_record(KINESIS_STREAM_NAME, message, KINESIS_PARTITION_KEY)
    print('{}/{} - {}'.format(i + 1, MESSAGE_COUNT, message))
