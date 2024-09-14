//package model.span.consts.consts
//
//
//import io.micrometer.core.instrument.{Counter, Timer}
//import model.span.Span
//import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
//import org.apache.kafka.streams.processor.{Cancellable, PunctuationType}
//import org.apache.kafka.streams.state.KeyValueStore
//import org.slf4j.LoggerFactory
//
//import java.time.{Clock, Duration, Instant}
//import java.util
//import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
//import java.util.stream.Collectors
//import java.util.{Collections, List, Objects}
//
//
///**
// * Receives spans keyed by trace_id and stores them. A {@link TraceEmitPunctuator} is scheduled to
// * run after the {@link RawSpansProcessor# groupingWindowTimeoutMs} interval to emit the trace. If
// * any spans for the trace arrive within the {@link RawSpansProcessor# groupingWindowTimeoutMs}
// * interval then the trace will get an additional {@link RawSpansProcessor# groupingWindowTimeoutMs}
// * time to accept spans.
// */
//object RawSpansProcessor {
//  private val logger = LoggerFactory.getLogger(classOf[RawSpansProcessor])
//  private val PROCESSING_LATENCY_TIMER = "hypertrace.rawspansgrouper.processing.latency"
//  private val ALL = "*"
//  private val tenantToSpansGroupingTimer = new ConcurrentHashMap[String, Timer]
//  // counter for number of spans dropped per tenant
//  private val droppedSpansCounter = new ConcurrentHashMap[String, Counter]
//  // counter for number of truncated traces per tenant
//  private val truncatedTracesCounter = new ConcurrentHashMap[String, Counter]
//  private val maxSpanCountMap = new util.HashMap[String, Long]
//}
//
//class RawSpansProcessor(private val clock: Clock) extends Processor[String, List[Span], String, List[Span]] {
//  private var context: ProcessorContext[String, List[Span]] = null
//  private var spanStore: KeyValueStore[SpanIdentity, RawSpan] = null
//  private var traceStateStore: KeyValueStore[TraceIdentity, TraceState] = null
//  private var peerIdentityToSpanMetadataStateStore: KeyValueStore[PeerIdentity, SpanMetadata] = null
//  private var groupingWindowTimeoutMs = 0L
//  private var dataflowSamplingPercent = -1
//  private var defaultMaxSpanCountLimit = Long.MAX_VALUE
//  private var traceEmitPunctuator: TraceEmitPunctuator = null
//  private var traceEmitTasksPunctuatorCancellable: Cancellable = null
//  private var peerCorrelationAgentTypeAttribute: String = null
//  private var peerCorrelationEnabledCustomers: util.List[String] = null
//  private var peerCorrelationEnabledAgents: util.List[String] = null
//  private var traceLatencyMeter: TraceLatencyMeter = null
//
//  override def init(context: ProcessorContext[TraceIdentity, StructuredTrace]): Unit = {
//    this.context = context
//    this.spanStore = context.getStateStore(SPAN_STATE_STORE_NAME)
//    this.traceStateStore = context.getStateStore(TRACE_STATE_STORE)
//    this.peerIdentityToSpanMetadataStateStore = context.getStateStore(PEER_IDENTITY_TO_SPAN_METADATA_STATE_STORE)
//    val jobConfig = context.appConfigs.get(RAW_SPANS_GROUPER_JOB_CONFIG).asInstanceOf[Config]
//    this.peerCorrelationAgentTypeAttribute = if (jobConfig.hasPath(PEER_CORRELATION_AGENT_TYPE_ATTRIBUTE_CONFIG)) jobConfig.getString(PEER_CORRELATION_AGENT_TYPE_ATTRIBUTE_CONFIG)
//    else null
//    this.peerCorrelationEnabledCustomers = if (jobConfig.hasPath(PEER_CORRELATION_ENABLED_CUSTOMERS)) jobConfig.getStringList(PEER_CORRELATION_ENABLED_CUSTOMERS)
//    else Collections.emptyList
//    this.peerCorrelationEnabledAgents = if (jobConfig.hasPath(PEER_CORRELATION_ENABLED_AGENTS)) jobConfig.getStringList(PEER_CORRELATION_ENABLED_AGENTS)
//    else Collections.emptyList
//    this.groupingWindowTimeoutMs = jobConfig.getLong(SPAN_GROUPBY_SESSION_WINDOW_INTERVAL_CONFIG_KEY) * 1000
//    if (jobConfig.hasPath(DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY) && jobConfig.getDouble(DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY) > 0 && jobConfig.getDouble(DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY) <= 100) this.dataflowSamplingPercent = jobConfig.getDouble(DATAFLOW_SAMPLING_PERCENT_CONFIG_KEY)
//    this.traceLatencyMeter = new TraceLatencyMeter(dataflowSamplingPercent)
//    if (jobConfig.hasPath(INFLIGHT_TRACE_MAX_SPAN_COUNT)) {
//      val subConfig = jobConfig.getConfig(INFLIGHT_TRACE_MAX_SPAN_COUNT)
//      subConfig.entrySet.forEach((entry: util.Map.Entry[String, ConfigValue]) => RawSpansProcessor.maxSpanCountMap.put(entry.getKey, subConfig.getLong(entry.getKey)))
//    }
//    if (jobConfig.hasPath(DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT)) defaultMaxSpanCountLimit = jobConfig.getLong(DEFAULT_INFLIGHT_TRACE_MAX_SPAN_COUNT)
//    val traceEmitPunctuatorStore = context.getStateStore(TRACE_EMIT_PUNCTUATOR_STORE_NAME)
//    traceEmitPunctuator = new TraceEmitPunctuator(new ThrottledPunctuatorConfig(jobConfig.getConfig(KAFKA_STREAMS_CONFIG_KEY), TRACE_EMIT_PUNCTUATOR), traceEmitPunctuatorStore, context, spanStore, traceStateStore, OUTPUT_TOPIC_PRODUCER, groupingWindowTimeoutMs, dataflowSamplingPercent)
//    // Punctuator scheduled on stream time => no input messages => no emits will happen
//    // We will almost never have input down to 0, i.e., there are no spans coming to platform,
//    // While using wall clock time handles that case, there is an issue with using wall clock.
//    // In cases of lag being burnt, we are processing message produced at different time stamp
//    // intervals, probably at higher rate than which they were produced, now not doing punctuation
//    // often will increase the amount of work yielding punctuator in next iterations and will keep
//    // on piling up until lag is burnt completely and only then the punctuator will catch up back to
//    // normal input rate. This is undesirable, here the outputs are only emitted from punctuator.
//    // If we burn lag from input topic, we want to push it down to output & downstream as soon
//    // as possible, if we hog it more and more it will delay cascading lag to downstream. Given
//    // grouper stays at start of pipeline and also that input dying down almost never happens
//    // it is better to use stream time over wall clock time for yielding trace emit tasks punctuator
//    traceEmitTasksPunctuatorCancellable = context.schedule(jobConfig.getDuration(TRACE_EMIT_PUNCTUATOR_FREQUENCY_CONFIG_KEY), PunctuationType.STREAM_TIME, traceEmitPunctuator)
//  }
//
//  override def process(record: Record[TraceIdentity, RawSpan]): Unit = {
//    val start = Instant.now
//    val currentTimeMs = clock.millis
//    val key = record.key
//    val value = record.value
//    var traceState = traceStateStore.get(key)
//    if (shouldDropSpan(key, traceState)) return
//    val event = value.getEvent
//    // spans whose trace is not created by ByPass flow, their trace will be created in below
//    // function call. Those spans' trace will not be created by TraceEmitPunctuator
//    if (isPeerServiceNameIdentificationRequired(event)) {
//      processSpanForPeerServiceNameIdentification(key, value, traceState, currentTimeMs)
//      return
//    }
//    val firstEntry = traceState == null
//    val tenantId = key.getTenantId
//    val traceId = value.getTraceId
//    val spanId = event.getEventId
//    spanStore.put(new SpanIdentity(tenantId, traceId, spanId), value)
//    if (firstEntry) {
//      traceState = fastNewBuilder(classOf[TraceState.Builder]).setTraceStartTimestamp(currentTimeMs).setTraceEndTimestamp(currentTimeMs).setEmitTs(-1) // deprecated, not used anymore.setTenantId(tenantId).setTraceId(traceId).setSpanIds(List.of(spanId)).build
//      traceEmitPunctuator.scheduleTask(currentTimeMs + groupingWindowTimeoutMs, key)
//    }
//    else {
//      traceState.getSpanIds.add(spanId)
//      val prevScheduleTimestamp = traceState.getTraceEndTimestamp + groupingWindowTimeoutMs
//      traceState.setTraceEndTimestamp(currentTimeMs)
//      if (!traceEmitPunctuator.rescheduleTask(prevScheduleTimestamp, currentTimeMs + groupingWindowTimeoutMs, key)) RawSpansProcessor.logger.warn("Failed to proactively reschedule task on getting span for trace id {}, schedule already dropped!", HexUtils.getHex(traceState.getTraceId))
//    }
//    traceStateStore.put(key, traceState)
//    RawSpansProcessor.tenantToSpansGroupingTimer.computeIfAbsent(value.getCustomerId, (k: String) => PlatformMetricsRegistry.registerTimer(RawSpansProcessor.PROCESSING_LATENCY_TIMER, util.Map.of("tenantId", k))).record(Duration.between(start, Instant.now).toMillis, TimeUnit.MILLISECONDS)
//    // no need to do context.forward. the punctuator will emit the trace once it's eligible to be
//    // emitted
//  }
//
//  private def processSpanForPeerServiceNameIdentification(key: TraceIdentity, value: RawSpan, traceState: TraceState, currentTimeMs: Long): Unit = {
//    val event = value.getEvent
//    val tenantId = key.getTenantId
//    val traceId = value.getTraceId
//    val firstEntry = traceState == null
//    val maybeEnvironment = HttpSemanticConventionUtils.getEnvironmentForSpan(event)
//    if (SpanSemanticConventionUtils.isClientSpanForOCFormat(event.getAttributes.getAttributeMap)) handleClientSpan(tenantId, event, maybeEnvironment.orElse(null))
//    else handleServerSpan(tenantId, event, maybeEnvironment.orElse(null))
//    // create structured trace and forward
//    val timestamps = traceLatencyMeter.trackEndToEndLatencyTimestamps(currentTimeMs, if (firstEntry) currentTimeMs
//    else traceState.getTraceStartTimestamp)
//    val trace = StructuredTraceBuilder.buildStructuredTraceFromRawSpans(util.List.of(value), traceId, tenantId, timestamps)
//    context.forward(new Record[TraceIdentity, StructuredTrace](key, trace, currentTimeMs), OUTPUT_TOPIC_PRODUCER)
//  }
//
//  // put the peer service identity and corresponding service name in state store
//  private def handleClientSpan(tenantId: String, event: Event, environment: String): Unit = {
//    val maybeHostAddr = HttpSemanticConventionUtils.getHostIpAddress(event)
//    val maybePeerAddr = HttpSemanticConventionUtils.getPeerIpAddress(event)
//    val maybePeerPort = HttpSemanticConventionUtils.getPeerPort(event)
//    val serviceName = event.getServiceName
//    val peerIdentity = PeerIdentity.newBuilder.setIpIdentity(IpIdentity.newBuilder.setTenantId(tenantId).setEnvironment(environment).setHostAddr(maybeHostAddr.orElse(null)).setPeerAddr(maybePeerAddr.orElse(null)).setPeerPort(maybePeerPort.orElse(null)).build).build
//    if (PeerIdentityValidator.isValid(peerIdentity)) this.peerIdentityToSpanMetadataStateStore.put(peerIdentity, SpanMetadata.newBuilder.setServiceName(serviceName).setEventId(event.getEventId).build)
//  }
//
//  // get the service name for that peer service identity and correlate the current span
//  private def handleServerSpan(tenantId: String, event: Event, environment: String): Unit = {
//    val maybePeerAddr = HttpSemanticConventionUtils.getPeerIpAddress(event)
//    val maybeHostAddr = HttpSemanticConventionUtils.getHostIpAddress(event)
//    val maybeHostPort = HttpSemanticConventionUtils.getHostPort(event)
//    val peerIdentity = PeerIdentity.newBuilder.setIpIdentity(IpIdentity.newBuilder.setTenantId(tenantId).setEnvironment(environment).setHostAddr(maybePeerAddr.orElse(null)).setPeerAddr(maybeHostAddr.orElse(null)).setPeerPort(maybeHostPort.orElse(null)).build).build
//    if (PeerIdentityValidator.isValid(peerIdentity)) {
//      val spanMetadata = this.peerIdentityToSpanMetadataStateStore.get(peerIdentity)
//      if (Objects.nonNull(spanMetadata)) {
//        RawSpansProcessor.logger.debug("Adding {} as: {} from spanId: {} in spanId: {} with service name: {}", PEER_SERVICE_NAME, spanMetadata.getServiceName, HexUtils.getHex(spanMetadata.getEventId), HexUtils.getHex(event.getEventId), event.getServiceName)
//        event.getEnrichedAttributes.getAttributeMap.put(PEER_SERVICE_NAME, AttributeValueCreator.create(spanMetadata.getServiceName))
//      }
//    }
//  }
//
//  private def isPeerServiceNameIdentificationRequired(event: Event): Boolean = {
//    if (!this.peerCorrelationEnabledCustomers.contains(event.getCustomerId) && !this.peerCorrelationEnabledCustomers.contains(RawSpansProcessor.ALL)) return false
//    val agentType = SpanAttributeUtils.getStringAttributeWithDefault(event, this.peerCorrelationAgentTypeAttribute, null)
//    Objects.nonNull(agentType) && this.peerCorrelationEnabledAgents.contains(agentType)
//  }
//
//  private def shouldDropSpan(key: TraceIdentity, traceState: TraceState): Boolean = {
//    val inFlightSpansPerTrace = if (traceState != null) traceState.getSpanIds.size
//    else 0
//    val maxSpanCountTenantLimit = if (RawSpansProcessor.maxSpanCountMap.containsKey(key.getTenantId)) RawSpansProcessor.maxSpanCountMap.get(key.getTenantId)
//    else defaultMaxSpanCountLimit
//    if (inFlightSpansPerTrace >= maxSpanCountTenantLimit) {
//      if (RawSpansProcessor.logger.isDebugEnabled) RawSpansProcessor.logger.debug("Dropping span [{}] from tenant_id={}, trace_id={} after grouping {} spans", traceState.getSpanIds.stream.map(HexUtils.getHex).collect(Collectors.toList), key.getTenantId, HexUtils.getHex(key.getTraceId), traceState.getSpanIds.size)
//      // increment the counter for dropped spans
//      RawSpansProcessor.droppedSpansCounter.computeIfAbsent(key.getTenantId, (k: String) => PlatformMetricsRegistry.registerCounter(DROPPED_SPANS_COUNTER, util.Map.of("tenantId", k))).increment()
//      // increment the counter when the number of spans reaches the max.span.count limit.
//      if (inFlightSpansPerTrace == maxSpanCountTenantLimit) RawSpansProcessor.truncatedTracesCounter.computeIfAbsent(key.getTenantId, (k: String) => PlatformMetricsRegistry.registerCounter(TRUNCATED_TRACES_COUNTER, util.Map.of("tenantId", k))).increment()
//      // drop the span as limit is reached
//      return true
//    }
//    false
//  }
//
//  override def close(): Unit = {
//    traceEmitTasksPunctuatorCancellable.cancel()
//  }
//}
