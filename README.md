# kafka-jolokia-telegraf-collector
> Kafka broker JMX metric collection made easy with Telegraf

Parses Kafka Broker JMX metrics exposed via jolokia and converts them to a set of InfluxDB Line protocol metrics

Currently supports at least Kafka 0.9 and Influxdb 0.13. May support older Kafka but not tested

https://jolokia.org
https://docs.influxdata.com/influxdb/v0.13/write_protocols/line/
http://kafka.apache.org/090/documentation.html#monitoring

## Requirements

Install and configure `jolokia` to expose Kafka Broker JMX metrics.

Setting `KAFKA_OPTS=-javaagent:/path/to/jolokia/jolokia-1.3.3/agents/jolokia-jvm.jar` when launching Kafka broker should be enough

## Usage

### How to run the script
```python kafka_jolokia_reporter.py [--jolokia-host] [--jolokia-port] [--jolokia-context]```

- `--jolokia-host` defaults to `localhost`
- `--jolokia-port` defaults to 8778
- `--jolokia-context` defaults to `/jolokia`

Example:
```python kafka_jolokia_reporter.py --jolokia-host=localhost --jolokia-port=8778 --jolokia-context=/jolokia”```

### Configure script to pass metrics to telegraf

The collector script works with Telegraf exec plugin.

Example configuration
```
[[inputs.exec]]
    commands = ["python /path/to/kafka_jolokia_reporter.py"]
    data_format = "influx"
```
