import argparse
import httplib
import json
import re

# VERSION 0.1
# FROM https://github.com/paksu/kafka-jolokia-telegraf-translator

# Ignore any metric that contains these strings because we don't want to
# collect aggregated data
ignored_metrics = [
    'Percentile', 'Mean', 'Rate'
]


def tokenize_metric_path(path):
    """
    Tokenizes a metric path for InfluxDB
    The path is split into 3 parts: metric_name, tag and value_prefix

    Example:

    path = "kafka.server:delayedOperation=Fetch,name=NumDelayedOperations,type=DelayedOperationPurgatory"

    results in:
    - metric_name = kafka.server
    - tags = {'delayedOperation': 'Fetch', 'name': 'NumDelayedOperations'}
    - value_prefix = DelayedOperationPurgatory

    """
    metric_name, metric_path = path.split(":")
    tokens = {token.split("=")[0]: token.split("=")[1] for token in metric_path.split(',')}

    # the 'type' field from the metric path as value_prefix, rest of the tokens in the metric path are tags
    value_prefix = tokens.pop('type')

    return metric_name, value_prefix, tokens


def get_metrics(value_prefix, values):
    """
    Gets a list  a jolokia response valueset into InfluxDB value format. Any non-numeric values will be skipped.

    Example:

    value_prefix = "foo"
    values = { "Value":1233.2 }

    is translated to: ["foo=233.2"]

    value_prefix = "foo"
    values = { "Count":0, "RateUnit":"SECONDS", "MeanRate":4.0 }

    is translated to: ["foo.Count=0", "foo.MeanRate=4.0"]
    """
    # Strip non-numeric values from the valueset
    values = {k: v for k, v in values.iteritems() if isinstance(v, (int, long, float, complex))}
    if 'Value' in values:
        # If the valueset contains only a single Value then use that
        return [(value_prefix, values['Value'])]
    else:
        # If the valueset contains multiple values then append the metric names to value_prefix
        # and combine them with the actual numeric value
        return [("{}.{}".format(value_prefix, value_suffix), value) for value_suffix, value in values.iteritems()]


def translate_values(metric, values):
    """
    Translates the given metric and valueset to an array of InfluxDB line protocol metrics


    metric = "kafka.server:delayedOperation=Fetch,name=NumDelayedOperations,type=DelayedOperationPurgatory"
    values = { "Value":1233.2 }

    results to:
    [
        kafka.server,delayedOperation=Fetch,name=NumDelayedOperations DelayedOperationPurgatory=1233.2
    ]

    metric = "kafka.server:delayedOperation=Fetch,name=NumDelayedOperations,type=DelayedOperationPurgatory"
    values = { "Count":1233.2, "SomeMetric":123 }

    results to:
    [
        kafka.server,delayedOperation=Fetch,name=NumDelayedOperations DelayedOperationPurgatory.Count=1233.2
        kafka.server,delayedOperation=Fetch,name=NumDelayedOperations DelayedOperationPurgatory.SomeMetric=123
    ]

    """
    metric_name, value_prefix, tags = tokenize_metric_path(metric)
    tag_string = ','.join(['{}={}'.format(k, v) for k, v in tags.iteritems()])
    tag_string = ',' + tag_string if tag_string else ''

    ignore_regex = None
    if ignored_metrics:
        ignore_regex = re.compile('|'.join(ignored_metrics))

    metrics = []
    for value_name, value in get_metrics(value_prefix, values):
        if not ignore_regex or not ignore_regex.search(value_name):
            metrics.append("{}{} {}={}".format(metric_name, tag_string, value_name, value))

    return metrics


def fetch_jmx_from_jolokia(host, port, jolokia_context, metric):
    """
    Fetches the given metric string from jolokia
    """
    conn = httplib.HTTPConnection(host, port)

    # append trailing slash
    if jolokia_context[-1] != "/":
        jolokia_context = jolokia_context + "/"

    path = "{}/read/{}".format(jolokia_context, metric)

    conn.request("GET", path)
    response = conn.getresponse()
    assert response.status == 200
    return response.read()


def translate_response(response, ignored_metrics=[]):
    """
    Parses a Kafka JMX metrics response from jolokia and converts it to set of InfluxDB Line protocol

    Currently supports at least Kafka 0.9 and Influxdb 0.13
    https://jolokia.org
    https://docs.influxdata.com/influxdb/v0.13/write_protocols/line/
    http://kafka.apache.org/090/documentation.html#monitoring
    """
    response = json.loads(response)

    if 'value' not in response:
        return []

    # Check if the response is a flat data structure containing values for only a single metric
    value_types = [type(value) for value in response['value'].values()]
    if dict not in value_types:
        # No nested data found in the values so we are dealing with a single metric
        metric = response['request']['mbean']
        return translate_values(metric, response['value'])
    else:
        # This is a multi-value response containing multiple metrics
        metrics = []
        for metric, value in response['value'].iteritems():
            metrics = metrics + translate_values(metric, value)
        return metrics

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--jolokia-host', default='localhost', help='Jolokia host')
    parser.add_argument('--jolokia-port', type=int, default=8778, help='Jolokia port')
    parser.add_argument('--jolokia-context', default='/jolokia', help='Jolokia context')
    args = parser.parse_args()

    # Collect these metrics
    metrics = [
        'kafka.server:*',
        'kafka.controller:*',
        'kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce',
        'kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchConsumer',
        'kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchFollower',
        'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce',
        'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer',
        'kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchFollower',
        'kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent',
        'kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs',
    ]
    for metric in metrics:
        response = fetch_jmx_from_jolokia(args.jolokia_host, args.jolokia_port, args.jolokia_context, metric)
        for line in translate_response(response):
            print line
