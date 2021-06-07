package org.kafkalytic.plugin

fun main(argv: Array<String>) {
//    val cl = AdminClient.create(Cluster("scoreu01.uk.db.com:9093",
//            "c:/temp/scoreu01.uk.db.com.jks", "sdcSd/dU0ICbqdir+mI5UA",
//            "c:/temp/scoreu01.uk.db.com.jks", "sdcSd/dU0ICbqdir+mI5UA"
//            ).toProperties())
//    println (cl.describeCluster().brokers().get())
//    val consumer = KafkaConsumer<Any, Any>(mapOf("bootstrap.servers" to "localhost:9092",
//            "group.id" to "test",
//            "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
//            "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer"))
//    consumer.subscribe(".*".toRegex().toPattern())
//    consumer.poll(100)

//    println(consumer.assignment().map{ consumer.position(it)})
    println("test")
    println("""([a-zA-Z0-9.-]+:[0-9]{1,5},)*([a-zA-Z0-9-]+\.)*([a-zA-Z0-9-])+:[0-9]{1,5}""".toRegex().matches("dd.ss:80,s-12s.com:8181"))
    println("""([a-zA-Z0-9.-]+:[0-9]{1,5},)*([a-zA-Z0-9-]+\.)*([a-zA-Z0-9-])+:[0-9]{1,5}""".toRegex().matches("s-12s:80,5-5d.dd.ss.:80,s-12s.com:8181"))
//    println("""([a-zA-Z0-9.-]+:[0-9]{1,5},)*([a-zA-Z0-9-]+\.)*([a-zA-Z0-9-])+:[0-9]{1,5}""".toRegex().matches(null))
    //([a-zA-Z0-9.-]+:[0-9]{1,5},)*[a-zA-Z0-9.-]+:[0-9]{1,5}
    //.toRegex().matches("ff:80,dd.com:8182")​​​​​​​​​
//    """(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9]{1,3}:\d{1,5}""".toRegex()
}