# GEIST GCP Connectors
<div>

[![Go Report Card](https://goreportcard.com/badge/github.com/zpiroux/geist-connector-gcp)](https://goreportcard.com/report/github.com/zpiroux/geist-connector-gcp)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist-connector-gcp&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist-connector-gcp)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist-connector-gcp&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist-connector-gcp)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist-connector-gcp&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist-connector-gcp)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=zpiroux_geist-connector-gcp&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=zpiroux_geist-connector-gcp)

</div>

## Usage

See [GEIST core repo](https://github.com/zpiroux/geist) for general information.

Note:
* The BigQuery connector has been moved out to its own repo [here](https://github.com/zpiroux/geist-connector-bigquery).
* The Bigtable connector has been moved out to its own repo [here](https://github.com/zpiroux/geist-connector-bigtable).

## Limitations and improvement areas

### Pubsub extractor DLQ
The Kafka Extractor supports automated DLQ handling of unretryable events (e.g. corrupt events that can't be transformed or processed by the Sink), if that option (`"dlq"`) is chosen in the Stream Spec, but the Pubsub Extractor currently only supports the options `"discard"` and `"fail"`.

Using Pubsub's built-in dead-letter topic option in the subscription as a work-around until this feature is added in the Extractor will currently not give the functionality as provided natively with Geist.

### Pubsub extractor microbatching
Also in contrast to Geist Kafka connector, the Pubsub extractor do not yet support the microbatch option as enabled for a stream with the stream spec field `ops.microBatch`.

## Contact
info @ zpiroux . com

## License
Geist GCP connectors source code is available under the MIT License.