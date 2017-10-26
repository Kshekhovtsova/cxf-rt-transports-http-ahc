package ru.yuksanbo.cxf.transportahc

import org.apache.cxf.BusFactory
import org.apache.cxf.continuations.ContinuationProvider
import org.apache.cxf.testutil.common.AbstractBusClientServerTestBase
import org.apache.hello_world_soap_http.Greeter
import org.apache.hello_world_soap_http.SOAPService
import org.apache.hello_world_soap_http.types.GreetMeResponse
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.util.concurrent.ExecutionException
import javax.xml.ws.Endpoint

class AhcHttpConduitTest : AbstractBusClientServerTestBase() {

    companion object {
        val port = "1080" //allocatePort(AhcHttpConduitTest::class.java)

        internal var ep: Endpoint? = null
        lateinit var request: String
        lateinit var g: Greeter

        @BeforeClass
        @Throws(Exception::class)
        @JvmStatic
        fun start() {
            val b = AbstractBusClientServerTestBase.createStaticBus()
            b.setProperty(AhcHttpConduit.Properties.Async, true)

            BusFactory.setThreadDefaultBus(b)

            ep = Endpoint.publish("http://localhost:$port/SoapContext/SoapPort",
                    object : org.apache.hello_world_soap_http.GreeterImpl() {
                        override fun greetMeLater(cnt: Long): String? {
                            //use the continuations so the async client can

                            //have a ton of connections, use less threads
                            //
                            //mimic a slow server by delaying somewhere between
                            //1 and 2 seconds, with a preference of delaying the earlier
                            //requests longer to create a sort of backlog/contention
                            //with the later requests
                            val p = context.messageContext[ContinuationProvider::class.java.name] as ContinuationProvider
                            val c = p.continuation
                            if (c.isNew) {
                                if (cnt < 0) {
                                    c.suspend(-cnt)
                                } else {
                                    c.suspend(2000 - cnt % 1000)
                                }
                                return null
                            }
                            return "Hello, finally! " + cnt
                        }

                        override fun greetMe(me: String?): String {
                            return "Hello " + me!!
                        }
                    })

            val builder = StringBuilder("NaNaNa")
            for (x in 0..49) {
                builder.append(" NaNaNa ")
            }
            request = builder.toString()


            val soapServiceWsdl = AhcHttpConduitTest::class.java.getResource("/wsdl/hello_world.wsdl")

            Assert.assertNotNull("WSDL is null", soapServiceWsdl)

            val service = SOAPService(soapServiceWsdl)
            Assert.assertNotNull("Service is null", service)

            g = service.soapPort
            Assert.assertNotNull("Port is null", g)
        }

        @AfterClass
        @Throws(Exception::class)
        @JvmStatic
        fun stop() {
            (g as java.io.Closeable).close()
            ep!!.stop()
            ep = null
        }
    }

    //todo: more tests
    @Test
    @Throws(Exception::class)
    fun testCallAsync() {
        updateAddressPort(g, port)
        val resp = g.greetMeAsync(request) { res ->
            try {
                res.get().responseType
            } catch (e: InterruptedException) {
                // TODO Auto-generated catch block
                e.printStackTrace()
            } catch (e: ExecutionException) {
                // TODO Auto-generated catch block
                e.printStackTrace()
            }
        }.get() as GreetMeResponse
        Assert.assertEquals("Hello " + request, resp.responseType)

        g.greetMeLaterAsync(1000) { }.get()
    }
}