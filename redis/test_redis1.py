import redis
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
# r = redis.Redis(connection_pool=pool)
def load(pdt_dict):
    """
    Load data into redis.

    Parameters
    ----------
    pdt_dict : Dict[str, str]
        To be stored in Redis
    """

    
    redIs = redis.Redis(connection_pool=pool)
    pipe = redIs.pipeline()
    for key in pdt_dict.keys():
        # pipe.hmset(self.seller + ":" + str(key), pdt_dict[key])
        pipe.set(str(key), pdt_dict[key]).incr('auto_number').execute()
    pipe.execute()
    print("--set---")

if __name__ == '__main__':
    pdt_dict = dict(name = "John", age = 36, country = "Norway")
    load(pdt_dict)