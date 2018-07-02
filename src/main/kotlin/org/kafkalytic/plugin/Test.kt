package org.kafkalytic.plugin

import com.google.gson.Gson
import org.apache.kafka.clients.admin.AdminClient

fun main(argv: Array<String>) {
//    val cl = AdminClient.create(Cluster("scoreu01.uk.db.com:9093",
//            "c:/temp/scoreu01.uk.db.com.jks", "sdcSd/dU0ICbqdir+mI5UA",
//            "c:/temp/scoreu01.uk.db.com.jks", "sdcSd/dU0ICbqdir+mI5UA"
//            ).toProperties())
//    println (cl.describeCluster().brokers().get())
    println(Gson().toJson(mapOf("1" to "a", "2" to "b")))

}