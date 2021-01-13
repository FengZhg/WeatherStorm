import json
import urllib
from copy import deepcopy
import time
from func_timeout import func_timeout, FunctionTimedOut, func_set_timeout
import msgpack
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.future import log
import threading
from threading import Timer
import asyncio
import requests


class DataRequire:
    customer_url = 'localhost:9092'
    req_semaphore = threading.BoundedSemaphore(1)  # 使用信号量，避免在同一时间段重复进行请求
    # 将从 json 中读入的城市数据存入类静态变量中
    city_data = json.loads(open('./citys.txt', 'r', encoding='GBK').read())
    req_data = {}  # 从接口请求来的数据暂存在本地缓存中
    producerPool = []  # 生产者对象池
    pool_req_semaphore = threading.BoundedSemaphore(1)  # 是否允许从生产者对象池请求生产正对象时使用，保证同时只有一个函数在请求生产正对象

    # 定义一个发送成功的回调函数
    def on_send_success(record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    # 定义一个发送失败的回调函数
    def on_send_error(excp):
        log.error('I am an errback', exc_info=excp)
        # handle exception

    # 定义请求生产生对象函数，并设置timeout时间,防止过多任务都在等待生产者对象
    @func_set_timeout(2000)
    def get_producer(self):
        # 将timeout设置为-1，当请求不到信号量的时候将会一直等待，如果超时会触发该函数设置的timeout
        self.pool_req_semaphore.acquire(blocking=True, timeout=-1)
        while (len(self.producerPool) == 0):
            pass
        producer = self.producerPool.pop()
        if (not isinstance(producer, KafkaProducer)):
            raise TypeError("获取对象时，生产者对象损坏")
            # 直接返回新的生产者对象
            return KafkaProducer(bootstrap_servers=[self.customer_url], retries=5)
        self.pool_req_semaphore.release()
        return producer

    def reset_producker(self, producer):
        # 进行生产者对象检验
        if (not isinstance(producer, KafkaProducer)):
            raise TypeError("归还对象时，生产者对象损坏")
            # 当原有归还对象损坏，创建新生产者对象进行返回
            self.producerPool.append(KafkaProducer(bootstrap_servers=[self.customer_url], retries=5))
            return
        self.producerPool.append(producer)

    def startSend(self, key, content) -> bool:  # 返回请求
        try:
            producer = self.get_producer()
        except FunctionTimedOut:
            print("请求生产者对象超时\n")
            return False
        except Exception as e:
            print("请求生产者对象未知错误\n")
            return False
        producer.send(key=key, value=content).add_callback(self.on_send_success).add_errback(self.on_send_error)
        # 把异步发送的数据全部发送出去
        producer.flush()
        self.reset_producker(producer)
        return True

    def require_remote_data(self,province_name, city_name):
        # 从定时任务获得所需请求的city_name
        # 使用的接口：腾讯接口 文档：https://github.com/bestyize/MiniWeather
        url = 'https://wis.qq.com/weather/common?' + urllib.parse.urlencode(
            {"source": "pc", "weather_type": "observe", "city": city_name,"province":province_name})
        print(url)
        response = requests.get(url)
        print(response.text)
        if (response.status_code != 200):
            raise Exception("网络请求异常")
            return
        result_dict = json.loads(response.text)

        # 舍弃接口返回的大部分信息，保留当前的观测信息，判断当前信息是否最新
        if (city_name not in self.req_data.keys() or
                self.req_data[city_name]["observe"]["update_time"] < result_dict["data"]["observe"]["update_time"]):
            # 更新当前观测信息
            print(result_dict)
            self.req_data[city_name] = deepcopy(result_dict["data"]["observe"])
            repeat_times = 0
            sealizer_content = json.dumps(result_dict["data"])
            while (not self.startSend(city_name, sealizer_content) and repeat_times < 20):
                repeat_times += 1

    def processCityJson(self):
        # 从信号量确定当前时间是否存在正在请求的协程
        if (not self.req_semaphore.acquire(blocking=True, timeout=2000)):  # 在请求信号量的时候进行阻塞，以实现timeout
            print("数据请求定时任务出错")
            return  # 当无法正差获得锁时，终止当前进程,并输出错误提示

        task = []  # 创建用于存放异步任务的list
        for i in self.city_data["provinces"]:
            for j in i["citys"]:
                task.append(asyncio.ensure_future(self.require_remote_data(i["provinceName"][:-1],j["citysName"][:-1])))
        asyncio.get_event_loop().run_until_complete(task)
        self.req_semaphore.release()

    def start(self):
        # 开始初始化生产者对象池
        for _ in range(200):
            # 创建了一个生产者的对象 定义重试的次数为5
            self.producerPool.append(KafkaProducer(bootstrap_servers=[self.customer_url], retries=5))

        # 开始定义声明并启动周期循环时间
        timerLoop = Timer(3600, self.processCityJson())
        timerLoop.start()


if __name__ == '__main__':
    # 测试用调试数据
    dr = DataRequire()
    dr.processCityJson()
