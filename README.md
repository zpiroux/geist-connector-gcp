# GEIST GCP Connectors
## Usage
See [GEIST core repo](https://github.com/zpiroux/geist).

## Limitations and improvement areas

### Pubsub extractor DLQ
The Kafka Extractor supports automated DLQ handling of unretryable events (e.g. corrupt events that can't be transformed or processed by the Sink), if that option (`"dlq"`) is chosen in the Stream Spec, but the Pubsub Extractor currently only supports the options `"discard"` and `"fail"`.

Using Pubsub's built-in dead-letter topic option in the subscription as a work-around until this feature is added in the Extractor will currently not have the intended effect.

### Pubsub extractor microbatching
Also in contrast to Geist Kafka connector, the Pubsub extractor do not yet support the microbatch option as enabled for a stream with the stream spec field `ops.micrBatch`.

## Contact
info @ zpiroux . com

## License
Geist GCP connectors source code is available under the MIT License.