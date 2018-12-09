import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import java.io.Closeable

@Suppress("CanBeParameter")
class Producer(
    private val exchangeName: String = "",
    private val exchangeType: BuiltinExchangeType = BuiltinExchangeType.DIRECT
) : Closeable {

    private val connection = ConnectionFactory().newConnection()
    private val channel: Channel = connection.createChannel()

    init {
        if (exchangeName != "") // don't "declare" the default exchange
            channel.exchangeDeclare(exchangeName, exchangeType, true)
    }

    fun produce(msg: String, routingKey: String) {
        channel.basicPublish(exchangeName, routingKey, null, msg.toByteArray())
    }

    override fun close() {
        connection.close()
        // (channels are closed automatically with the connection)
    }
}
