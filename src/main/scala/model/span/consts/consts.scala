package model.span.consts

object consts {

  val INPUT_TOPIC_CONFIG_KEY = "input.topic"
  val OUTPUT_TOPIC_CONFIG_KEY = "output.topic"
  val SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY = "span.groupby.session.window.interval"
  val RAW_SPANS_GROUPER_JOB_CONFIG = "raw-spans-grouper-job-config"
  val SPAN_STATE_STORE_NAME = "span-data-store"
  val TRACE_STATE_STORE = "trace-state-store"
  val PEER_IDENTITY_TO_SPAN_METADATA_STATE_STORE = "peer-identity-to-span-metadata-store"
  val OUTPUT_TOPIC_PRODUCER = "output-topic-producer"
  val SPANS_PER_TRACE_METRIC = "spans_per_trace"
  val TRACE_CREATION_TIME = "trace.creation.time"
  val DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY = "dataflow.metriccollection.sampling.percent"
  val PEER_CORRELATION_AGENT_TYPE_ATTRIBUTE_CONFIG = "peer.correlation.agent.type.attribute"
  val PEER_CORRELATION_ENABLED_CUSTOMERS = "peer.correlation.enabled.customers"
  val PEER_CORRELATION_ENABLED_AGENTS = "peer.correlation.enabled.agents"
  val INFLIGHT_TRACE_MAX_SPAN_COUNT = "max.span.count"
  val DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT = "default.max.span.count"
  val DROPPED_SPANS_COUNTER = "hypertrace.dropped.spans"
  val TRUNCATED_TRACES_COUNTER = "hypertrace.truncated.traces"
  val TRACE_EMIT_PUNCTUATOR = "trace-emit-punctuator"
  val TRACE_EMIT_PUNCTUATOR_STORE_NAME = "emitter-store"
  val TRACE_EMIT_PUNCTUATOR_FREQUENCY_CONFIG_KEY = "trace.emit.punctuator.frequency"
}
