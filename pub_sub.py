import logging
import json
import pika

from functools import partial
from settings import RABBITMQ_URL

_logger = logging.getLogger(__name__)


class Publish:
    def __init__(self, exchange_type, exchange_name):
        """
        :param exchange_type: 交换类型。广播：fanout，直连：direct，主题：topic
        :param exchange_name: 交换机名
        """
        params = pika.URLParameters(RABBITMQ_URL)
        self.connection = pika.BlockingConnection(parameters=params)
        self.channel = self.connection.channel()
        self.exchange = exchange_name
        self.exchange_type = exchange_type
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type
        )

    def send(self, body, routing_key='', delivery_mode=2,
             content_type='application/json', content_encoding='utf8'):
        """
        :param body: 消息
        :param routing_key: 路由键，交换类型为非广播时，需要指定
        :param delivery_mode: 2表示让消息持久化
        :param content_type: 文本：text/plain，JSON：application/json
        :param content_encoding: 编码格式
        :return:
        """
        if content_type == 'application/json':
            body = json.dumps(body)

        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=delivery_mode,
                content_type=content_type,
                content_encoding=content_encoding
            )
        )
        _logger.info('MSG: {}; EXCHANGE: {}; EXCHANGE_TYPE: {}; ROUTING_KEY: {}.'
                     .format(body, self.exchange, self.exchange_type, routing_key))
        self.channel.close()


def basic_publish(exchange_type, exchange_name, body, routing_key='',
                  delivery_mode=2, content_type='application/json', content_encoding='utf8'):
    Publish(exchange_type, exchange_name)\
        .send(body, routing_key, delivery_mode, content_type, content_encoding)
    return True


broadcast = partial(basic_publish, 'fanout')
topic_publish = partial(basic_publish, 'topic')
publish = partial(basic_publish, 'direct')


class Subscribe:
    def __init__(self, exchange_name, exchange_type, on_message=None, queue_name=None, routing_key='',
                 exclusive=True, durable=True, prefetch_count=1, no_ack=False):
        """
        :param exchange_type: 交换类型。广播：fanout，直连：direct，主题：topic
        :param exchange_name: 交换机名
        :param callback: 回调函数
        :param queue_name: 队列名
        :param routing_key: 路由键，交换类型为非广播时，需要指定
        :param exclusive: 是否排外。一，当连接关闭，队列是否自动删除；二，是否可以使用多个消费者访问同一个队列
        :param durable: 队列是否持久化，和发送消息时的delivery_mode参数配合使用
        :param prefetch_count: 同一时间一个队列处理的消息个数
        :param no_ack: worker不返回ACK
        """
        params = pika.URLParameters(RABBITMQ_URL)
        self.connection = pika.BlockingConnection(parameters=params)
        self.channel = self.connection.channel()

        self.exchange = exchange_name
        self.exchange_type = exchange_type
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type
        )

        # 创建queue
        if not queue_name:
            result = self.channel.queue_declare(exclusive=exclusive, durable=durable)
            queue_name = result.method.queue
        else:
            self.channel.queue_declare(queue=queue_name, exclusive=exclusive, durable=durable)

        # exchange和queue绑定
        if exchange_type == 'fanout':
            self.channel.queue_bind(exchange=exchange_name, queue=queue_name)
        else:
            self.channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

        self.channel.basic_qos(prefetch_count=prefetch_count)

        # 绑定处理消息的函数
        if on_message is None or not callable(on_message):
            on_message = Subscribe.default_on_message
        self.channel.basic_consume(on_message, queue=queue_name, no_ack=no_ack)

    def consuming(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()

        self.connection.close()

    @staticmethod
    def default_on_message(ch, method, properties, body):
        _logger.info('Consuming: {}'.format(body))
        _logger.info('Exchange: {}; Routing_key: {}'.format(method.exchange, method.routing_key))
        _logger.info('Properties: {}'.format(properties))
        if properties.content_type == 'application/json':
            body = json.loads(body)
            _logger.info('Body: {}'.format(body))
        ch.basic_ack(delivery_tag=method.delivery_tag)


def subscribe(func, exchange_name, exchange_type, queue_name=None, routing_key='',
              exclusive=True, durable=True, prefetch_count=1, no_ack=False):
    Subscribe(exchange_name, exchange_type, func, queue_name, routing_key,
              exclusive, durable, prefetch_count, no_ack).consuming()
