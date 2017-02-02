package ru.yuksanbo.cxf.transportahc


class HttpException(
        responseCode: Int,
        statusMsg: String,
        url: String
) : RuntimeException("Response from '$url' failed with code '$responseCode' and message '$statusMsg'")