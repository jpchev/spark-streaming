import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from functools import reduce
from pymemcache.client import base
from pymemcache import serde

running_state_init_max = {
    "bpi" : {
        "EUR": {
            "rate_float": sys.float_info.min
        }
    }
}

running_state_init_min = {
    "bpi" : {
        "EUR": {
            "rate_float": sys.float_info.max
        }
    }
}

def mapData(d_text):
    d = json.loads(d_text)
    # extract only date
    key = d["time"]["updatedISO"][0:10]
    return (key, d)

def maxF(a, b):
    a_rate = a["bpi"]["EUR"]["rate_float"]
    b_rate = b["bpi"]["EUR"]["rate_float"]
    return b if a_rate <= b_rate else a

def minF(a, b):
    a_rate = a["bpi"]["EUR"]["rate_float"]
    b_rate = b["bpi"]["EUR"]["rate_float"]
    return a if a_rate <= b_rate else b

def updateFunction(new_values, running_state):
    if len(new_values) == 0:
        return running_state

    return {
        "max": maxF(reduce(maxF, new_values), running_state["max"] if running_state else running_state_init_max),
        "min": minF(reduce(minF, new_values), running_state["min"] if running_state else running_state_init_min)
    }

if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise IOError("Invalid usage; the correct format is:\nbitcoin_analyse.py <hostname> <port>")

    spc = SparkContext(appName="bitcoinAnalyse")

    # Create a StreamingContext with a batch interval of 2 seconds
    stc = StreamingContext(spc, 2)

    # Checkpointing feature
    stc.checkpoint("checkpoint")

    # Creating a DStream to connect to hostname:port (like localhost:9999)
    lines = stc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    # create keys by day, update state
    dstream = lines.map(mapData).updateStateByKey(updateFunction)

    # Print the current state
    #dstream.pprint()

    class JsonSerde(object):
        def serialize(self, key, value):
            if isinstance(value, str):
                return value, 1
            return json.dumps(value), 2

        def deserialize(self, key, value, flags):
           if flags == 1:
               return value
           if flags == 2:
               return json.loads(value)
           raise Exception("Unknown serialization format")

    def handlePartition(iter):
        client = base.Client('localhost', serde=serde.pickle_serde)
        for record in iter:
            stats = client.get("bitcoin_stats")
            day = record[0]
            stats[day] = record[1]
            client.set("bitcoin_stats", stats)

    dstream.foreachRDD(lambda rdd: rdd.foreachPartition(handlePartition))

    # start the computation
    stc.start()
    
    # Wait for the computation to terminate
    stc.awaitTermination()
