package ru.yuksanbo.cxf.transportahc

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicBoolean
import org.asynchttpclient.request.body.generator.ByteArrayBodyGenerator

internal class AhcOutputStream(
        val maxBufferSize: Long,
        val requestContext: AhcRequestContext,
        val sendMessageOp: (AhcRequestContext) -> Unit
) : ByteArrayOutputStream() {

    private val closed = AtomicBoolean(true)

    /**
     * Performs message sending
     */
    override fun close() {
        if (!closed.compareAndSet(true, false)) {
            throw IllegalStateException("Method close already called")
        }

        requestContext.ahcRequestBuilder.setBody(ByteArrayBodyGenerator(toByteArray()))
        sendMessageOp.invoke(requestContext)
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        checkNotClosed()
        checkBufferSize(count + len)
        super.write(b, off, len)
    }

    override fun write(b: ByteArray) {
        checkNotClosed()
        checkBufferSize(count + b.size)
        super.write(b)
    }

    override fun write(b: Int) {
        checkNotClosed()
        checkBufferSize(count + 1)
        super.write(b)
    }

    private fun checkNotClosed() {
        if (!closed.get()) {
            throw IllegalStateException("Can't modify output data, cause associated byte buffer still in use")
        }
    }

    private fun checkBufferSize(size: Int) {
        if (size > maxBufferSize) {
            throw MaxBufferSizeReachedException(maxBufferSize, "Per request buffer size")
        }
    }

}

