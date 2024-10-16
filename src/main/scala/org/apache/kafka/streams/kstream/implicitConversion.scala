package org.apache.kafka.streams.kstream

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.state.internals.CoralogixStore.WindowedSnapshotSupplier

object implicitConversion {

  implicit def windowStoreToSnapshotStore[K, V, S <: StateStore](implicit
                                                                 keySerde: Serde[K],
                                                                 valueSerde: Serde[V]
                                                                ): Materialized[K, V, S] = {
    val materialized = Materialized.`with`[K, V, S](keySerde, valueSerde)
    if (materialized.dslStoreSuppliers == null)
      materialized.dslStoreSuppliers = Utils.newInstance(classOf[BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers], classOf[DslStoreSuppliers])
    materialized.dslStoreSuppliers = new SnapshotStoreSupplier(materialized.dslStoreSuppliers)
    println("replacing windowStoreToSnapshotStore")
    materialized
  }

  implicit class SnapshotMaterialized[K, V, S <: StateStore](materialized: Materialized[K, V, S]) {
    def withSnapshotEnabled: Materialized[K, V, S] = {
      materialized.dslStoreSuppliers = new SnapshotStoreSupplier(materialized.dslStoreSuppliers)
      materialized
    }
  }

  class SnapshotStoreSupplier(innerSupplier: DslStoreSuppliers) extends DslStoreSuppliers() {
    override def keyValueStore(params: DslKeyValueParams): KeyValueBytesStoreSupplier = {
      innerSupplier.keyValueStore(params)
    }

    override def windowStore(params: DslWindowParams): WindowBytesStoreSupplier = {
      val innerStore = innerSupplier.windowStore(params)
      new WindowedSnapshotSupplier(innerStore.name(), innerStore.retentionPeriod(), innerStore.segmentIntervalMs(), innerStore.windowSize(), innerStore.retainDuplicates(), params.isTimestamped)
    }

    override def sessionStore(params: DslSessionParams): SessionBytesStoreSupplier = {
      innerSupplier.sessionStore(params)

    }
  }
}