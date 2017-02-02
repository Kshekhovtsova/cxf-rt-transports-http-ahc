package ru.yuksanbo.cxf.transportahc


class MaxBufferSizeReachedException(
        maxBufferSize: Long,
        subject: String
) : RuntimeException("'$subject' reached max buffer size of '$maxBufferSize'")