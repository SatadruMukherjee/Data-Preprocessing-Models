from time import sleep
from json import dumps
from kafka import KafkaProducer


#Lab 1: Write message to a partition (mentioning the partition number while publishing the message)

topic_name='hello_world1'
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
data1 = {'number' : 1}
data2 = {'number' : 2}
data3 = {'number' : 3}
data4 = {'number' : 4}
data5 = {'number' : 5}
data6 = {'number' : 6}
producer.send(topic_name, value=data1,partition=1)
producer.send(topic_name, value=data2,partition=1)
producer.send(topic_name, value=data3,partition=1)
producer.send(topic_name, value=data4,partition=2)
producer.send(topic_name, value=data5,partition=2)
producer.send(topic_name, value=data6,partition=0)
producer.close()

#Lab 2: Pass key value pair
from json import dumps
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic_name='hello_world2'
producer.send(topic_name, key=b'foo', value=b'bar') #Note :key & value serialization we are doing while publishing the message
                                                    #itself , so explicitly not mentioning the key or value serializer
producer.send(topic_name, key=b'foo', value=b'bar')
producer.close()

#Lab 3: Pass key value pair with key & value serialization with key or value serializer explicitly mentioned
from json import dumps
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],key_serializer=str.encode,value_serializer=lambda x: dumps(x).encode('utf-8'))
topic_name='hello_world3'
data1 = {'number' : 1}
data2 = {'number' : 2}
data3 = {'number' : 3}
data4 = {'number' : 4}
data5 = {'number' : 5}
data6 = {'number' : 6}
producer.send(topic_name,  key='ping',value=data1)
producer.send(topic_name, key='ping',value=data2)
producer.send(topic_name, key='ping',value=data3)
producer.send(topic_name, key='pong',value=data4)
producer.send(topic_name, key='pong',value=data5)
producer.send(topic_name, key='pong',value=data6)
producer.close()



#Lab 4: Customize a partitioner
from time import sleep
from json import dumps
from kafka import KafkaProducer


def custom_partitioner(key, all_partitions, available):
    """
    Customer Kafka partitioner to get the partition corresponding to key
    :param key: partitioning key
    :param all_partitions: list of all partitions sorted by partition ID
    :param available: list of available partitions in no particular order
    :return: one of the values from all_partitions or available
    """
    print("The key is  : {}".format(key))
    print("All partitions : {}".format(all_partitions))
    print("After decoding of the key : {}".format(key.decode('UTF-8')))
    return int(key.decode('UTF-8'))%len(all_partitions)


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],partitioner=custom_partitioner)
topic_name='hello_world4'
producer.send(topic_name, key=b'3', value=b'Hello Partitioner')
producer.send(topic_name, key=b'2', value=b'Hello Partitioner123')
producer.send(topic_name, key=b'369', value=b'Hello Partitioner')
producer.send(topic_name, key=b'301', value=b'Hello Partitioner')


