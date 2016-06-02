import unittest
import kafka_jolokia_reporter

single_metric_response = """
{
    "status": 200,
    "timestamp": 1464865032,
    "request": {
        "type": "read",
        "mbean": "kafka.network:name=TotalTimeMs,request=Produce,type=RequestMetrics"
    },
    "value": {
        "Count": 32247859,
        "Min": 0.0,
        "98thPercentile": 1.0,
        "95thPercentile": 1.0,
        "75thPercentile": 0.0,
        "999thPercentile": 9.1,
        "Mean": 0.2,
        "Max": 317.0,
        "99thPercentile": 1.0,
        "StdDev": 0.3,
        "50thPercentile": 0.0
    }
}
"""
multi_metric_response = """
{
   "timestamp":1464852410,
   "status":200,
   "request":{
      "mbean":"kafka.server:*",
      "type":"read"
   },
   "value":{
      "kafka.server:delayedOperation=Fetch,name=NumDelayedOperations,type=DelayedOperationPurgatory":{
         "Value":1233.2
      },
      "kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics":{
         "OneMinuteRate":1.0,
         "Count":0,
         "RateUnit":"SECONDS",
         "MeanRate":4.0,
         "FiveMinuteRate":0.2,
         "EventType":"bytes",
         "FifteenMinuteRate":0.3
      },
      "kafka.server:type=Produce":{
         "queue-size":0.0
      },
      "kafka.server:delayedOperation=Rebalance,name=PurgatorySize,type=DelayedOperationPurgatory":{
         "Value":0
      }
   }
}
"""


class TestTranslator(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_tokenize_metric_path(self):
        metric_name, value_name, tags = kafka_jolokia_reporter.tokenize_metric_path("kafka.server:delayedOperation=Fetch,name=NumDelayedOperations,type=DelayedOperationPurgatory")
        self.assertEqual(metric_name, "kafka.server")
        self.assertEqual(tags, {"delayedOperation": "Fetch", "name": "NumDelayedOperations"})
        self.assertEqual(value_name, "DelayedOperationPurgatory")

        metric_name, value_name, tags  = kafka_jolokia_reporter.tokenize_metric_path("kafka.server:networkProcessor=0,type=socket-server-metrics")
        self.assertEqual(metric_name, "kafka.server")
        self.assertEqual(tags, {"networkProcessor": "0"})
        self.assertEqual(value_name, "socket-server-metrics")

        metric_name, value_name, tags  = kafka_jolokia_reporter.tokenize_metric_path("kafka.server:name=MessagesInPerSec,topic=some_topic,type=BrokerTopicMetrics")
        self.assertEqual(metric_name, "kafka.server")
        self.assertEqual(tags, {"name": "MessagesInPerSec", "topic": "some_topic"})
        self.assertEqual(value_name, "BrokerTopicMetrics")

    def test_translates_multi_metric_response(self):
        translated_response = kafka_jolokia_reporter.translate_response(multi_metric_response)
        self.assertItemsEqual([
            "kafka.server,delayedOperation=Fetch,name=NumDelayedOperations DelayedOperationPurgatory=1233.2",
            "kafka.server,name=BytesOutPerSec BrokerTopicMetrics.Count=0",
            "kafka.server Produce.queue-size=0.0",
            "kafka.server,delayedOperation=Rebalance,name=PurgatorySize DelayedOperationPurgatory=0"
        ], translated_response)

    def test_translates_single_metric_response(self):
        translated_response = kafka_jolokia_reporter.translate_response(single_metric_response)
        self.assertItemsEqual([
            "kafka.network,request=Produce,name=TotalTimeMs RequestMetrics.Count=32247859",
            "kafka.network,request=Produce,name=TotalTimeMs RequestMetrics.Min=0.0",
            "kafka.network,request=Produce,name=TotalTimeMs RequestMetrics.Max=317.0",
            "kafka.network,request=Produce,name=TotalTimeMs RequestMetrics.StdDev=0.3",
        ], translated_response)

if __name__ == "__main__":
    unittest.main()
