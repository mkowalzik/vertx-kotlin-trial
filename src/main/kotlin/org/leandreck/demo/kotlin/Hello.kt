package org.leandreck.demo.kotlin

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.hazelcast.config.Config
import io.netty.util.CharsetUtil
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.json.Json
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.*


fun main(args: Array<String>) {
    val hazelcastConfig = Config()
    hazelcastConfig.instanceName = "King Bob Cluster"
    println("Starting Clustermanager...")
    val mgr = HazelcastClusterManager(hazelcastConfig)
    val options = VertxOptions().setClusterManager(mgr)
    Vertx.clusteredVertx(options) { res ->
        if (res.succeeded()) {
            println("Cluster started")
            val vertx = res.result()
            val eventBus = vertx.eventBus()
            println("We now have a clustered event bus: " + eventBus)
            eventBus.registerCodec(RequestMessageCodec())

            vertx.deployVerticle(MyFirstVerticle::class.java.name)
            vertx.deployVerticle(TimerVerticle::class.java.name)
        } else {
            // failed!
            println("Clusterstartup failed ${res.cause()}")
        }
    }
}


class RequestMessageCodec : MessageCodec<RequestMessage, RequestMessage> {

    override fun name(): String = "RequestMessageCodec"

    override fun systemCodecID(): Byte = -1

    override fun transform(s: RequestMessage?): RequestMessage? = s?.copy()

    override fun encodeToWire(buffer: Buffer, requestMessage: RequestMessage) {
        val strBytes = Json.encode(requestMessage).toByteArray(CharsetUtil.UTF_8)
        buffer.appendInt(strBytes.size)
        buffer.appendBytes(strBytes)
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): RequestMessage {
        val length = buffer.getInt(pos)
        val startPayload = pos + 4
        val bytes = buffer.getBytes(startPayload, startPayload + length)
        return Json.decodeValue(String(bytes, CharsetUtil.UTF_8), RequestMessage::class.java)
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class RequestMessage
@JsonCreator
constructor(@JsonProperty("msgId") val msgId: String,
            @JsonProperty("host") val host: String,
            @JsonProperty("port") val port: Int,
            @JsonProperty("uri") val uri: String)

class MyFirstVerticle : AbstractVerticle() {

    override fun start(future: Future<Void>) {
        val eventBus = vertx.eventBus()
        eventBus.consumer<Any>("send.request") { it -> receiveRequest(it) }
    }

    fun receiveRequest(message: Message<Any>) {
        println("I have received a message: ${message.body()}")

        val theRequest = message.body()
        if (theRequest is RequestMessage) {
            val start = System.currentTimeMillis()
            println("Start=$start")
            vertx.createHttpClient()
                    .getNow(theRequest.port, theRequest.host, theRequest.uri, { response -> println("Received response with status code ${response.statusCode()} and took ${System.currentTimeMillis() - start}ms") })
        }
    }

}

class TimerVerticle : AbstractVerticle() {

    override fun start(future: Future<Void>) {
        vertx.setPeriodic(10000, { it ->
            sendMessage()
        })
    }

    fun sendMessage() {
        val eventBus = vertx.eventBus()
        val requestMessage = RequestMessage(msgId = UUID.randomUUID().toString(), host = "www.golem.de", port = 80, uri = "/")
        println(requestMessage)
        eventBus.send("send.request", requestMessage, DeliveryOptions().setCodecName("RequestMessageCodec"))
    }
}