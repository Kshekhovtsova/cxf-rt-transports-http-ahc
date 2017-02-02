package ru.yuksanbo.cxf.transportahc

import io.netty.handler.codec.http.HttpHeaders
import org.asynchttpclient.Request
import org.asynchttpclient.RequestBuilder
import ru.yuksanbo.common.timings.Timings


data class AhcRequestContext(
        val address: String,
        val headers: HttpHeaders,
        val ahcRequestBuilder: RequestBuilder,
        val timings: Timings
) {
    var request: Request? = null
}