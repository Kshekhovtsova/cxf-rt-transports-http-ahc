package ru.yuksanbo.cxf.transportahc

import io.netty.buffer.ByteBuf
import org.asynchttpclient.request.body.Body
import org.asynchttpclient.request.body.generator.BodyGenerator
import java.io.IOException

internal class ByteArrayBodyGenerator(
        val bytes: ByteArray,
        val length: Int
) : BodyGenerator {

    inner class ByteBody : Body {
        private var eof = false
        private var lastPosition = 0

        override fun getContentLength(): Long {
            return length.toLong()
        }

        @Throws(IOException::class)
        override fun transferTo(target: ByteBuf): Body.BodyState {

            if (eof) {
                return Body.BodyState.STOP
            }

            val remaining = length - lastPosition
            val initialTargetWritableBytes = target.writableBytes()
            if (remaining <= initialTargetWritableBytes) {
                target.writeBytes(bytes, lastPosition, remaining)
                eof = true
            }
            else {
                target.writeBytes(bytes, lastPosition, initialTargetWritableBytes)
                lastPosition += initialTargetWritableBytes
            }
            return Body.BodyState.CONTINUE
        }

        @Throws(IOException::class)
        override fun close() {
            lastPosition = 0
            eof = false
        }
    }

    /**
     * {@inheritDoc}
     */
    override fun createBody(): Body {
        return ByteBody()
    }
}