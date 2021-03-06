from redis import StrictRedis
#from weixin.config import *
from pickle import dumps, loads
#from weixin.request import WeixinRequest
from request import WeixinRequest


class RedisQueue():
    def __init__(self):
        """
        初始化Redis
        """
        #self.db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
        self.db = StrictRedis(host="localhost", port=6379, password='123456')

    def add(self, request):
        """
        向队列添加序列化后的Request
        :param request: 请求对象
        :param fail_time: 失败次数
        :return: 添加结果
        """
        if isinstance(request, WeixinRequest):
            print("RedisQueue::add::request:", request)
            in_val=dumps(request)
            print("RedisQueue::add::dumps(request):", in_val)
            #return self.db.rpush(REDIS_KEY, in_val)
            print("key len in nosdql:", self.db.llen('weixin'))
            return self.db.rpush('weixin', in_val)
        return False

    def pop(self):
        """
        取出下一个Request并反序列化
        :return: Request or None
        """
        #if self.db.llen(REDIS_KEY):
        print("key len in nos:", self.db.llen('weixin'))
        if self.db.llen('weixin'):
            ret_val = loads(self.db.lpop('weixin'))
            print("RedisQueue::pop::lpop:", ret_val)
            return ret_val
        else:
            return False

    def clear(self):
        self.db.delete('weixin')

    def empty(self):
        return self.db.llen('weixin') == 0


if __name__ == '__main__':
    db = RedisQueue()
    start_url = 'http://www.baidu.com'
    weixin_request = WeixinRequest(url=start_url, callback='hello', need_proxy=True)
    db.add(weixin_request)
    request = db.pop()
    print(request)
    print(request.callback, request.need_proxy)
