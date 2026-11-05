package com.sdk.growthbook.network

import com.sdk.growthbook.utils.Resource
import com.sdk.growthbook.utils.SSEConnectionController
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.utils.io.ByteReadChannel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger

class GBNetworkDispatcherKtorSseHeartbeatTest {
    private val payload = """{"features":{"flag":{"defaultValue":true}}}"""

    @Test
    fun `empty data heartbeats keep quiet streams retryable without emitting empty features`() = runBlocking {
        val (result, attempts) = captureRotations(heartbeat = "data: \n\n")

        assertEquals(Resource.Success(payload), result)
        assertEquals(4, attempts)
    }

    @Test
    fun `comment heartbeats keep quiet streams retryable without emitting features`() = runBlocking {
        val (result, attempts) = captureRotations(heartbeat = ": keepalive\n\n")

        assertEquals(Resource.Success(payload), result)
        assertEquals(4, attempts)
    }

    @Test
    fun `repeated closed streams without heartbeat still exhaust retries`() = runBlocking {
        val (result, attempts) = captureRotations(heartbeat = "retry: 10000\n\n", sendFeature = false)

        assertTrue(result == null || result is Resource.Error)
        assertEquals(2, attempts)
    }

    @Test
    fun `heartbeat in unsuccessful response does not reset retries`() = runBlocking {
        val (result, attempts) = captureRotations(
            heartbeat = "data: \n\n",
            sendFeature = false,
            status = HttpStatusCode.ServiceUnavailable,
        )

        assertTrue(result == null || result is Resource.Error)
        assertEquals(2, attempts)
    }

    @Test
    fun `heartbeat in non SSE response does not reset retries`() = runBlocking {
        val (result, attempts) = captureRotations(
            heartbeat = "data: \n\n",
            sendFeature = false,
            contentType = "text/html",
        )

        assertTrue(result == null || result is Resource.Error)
        assertEquals(2, attempts)
    }

    @Test
    fun `incomplete empty data event does not reset retries`() = runBlocking {
        val (result, attempts) = captureRotations(heartbeat = "data: \n", sendFeature = false)

        assertTrue(result == null || result is Resource.Error)
        assertEquals(2, attempts)
    }

    private suspend fun captureRotations(
        heartbeat: String,
        sendFeature: Boolean = true,
        status: HttpStatusCode = HttpStatusCode.OK,
        contentType: String = "text/event-stream",
    ): Pair<Resource<String>?, Int> {
        val attempts = AtomicInteger()
        val engine = MockEngine {
            val current = attempts.incrementAndGet()
            val frame = if (sendFeature && current == 4) {
                "event: features\ndata: $payload\n\n"
            } else {
                heartbeat
            }
            respond(
                content = ByteReadChannel(frame),
                status = status,
                headers = headersOf(HttpHeaders.ContentType, contentType),
            )
        }
        val client = HttpClient(engine)
        try {
            val controller = SSEConnectionController().apply { start() }
            val result = withTimeout(5_000) {
                val flow = GBNetworkDispatcherKtor(
                    client = client,
                    maxRetries = 1,
                    initialRetryDelayMs = 1,
                    maxRetryDelayMs = 1,
                ).consumeSSEConnection(
                    url = "http://example.test/sub/test-key",
                    sseController = controller,
                )
                if (sendFeature) flow.first() else flow.toList().singleOrNull()
            }
            return result to attempts.get()
        } finally {
            client.close()
        }
    }
}
