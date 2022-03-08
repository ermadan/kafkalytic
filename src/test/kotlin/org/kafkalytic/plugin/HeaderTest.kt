package org.kafkalytic.plugin

import org.apache.kafka.common.header.internals.RecordHeader
import org.junit.Assert.assertEquals
import org.junit.Test
import java.nio.charset.Charset

class HeaderTest {
    @Test
    fun test() {
        assertEquals(listOf(RecordHeader("header1", ("v1:v2").toByteArray(Charset.defaultCharset())),
            RecordHeader("header2", ("v3").toByteArray(Charset.defaultCharset()))), createCustomHeader("""header1:v1:v2;header2:v3"""))
    }
}