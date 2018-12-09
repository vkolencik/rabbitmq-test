import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import java.io.Closeable

class Producer(
    private val exchangeName: String = "",
    exchangeType: BuiltinExchangeType = BuiltinExchangeType.DIRECT,
    vararg bindings: Binding
) : Closeable {

    private val connection = ConnectionFactory().newConnection()
    private val channel: Channel = connection.createChannel()

    init {
        if (exchangeName != "") // don't "re-declare" the default exchange
            channel.exchangeDeclare(exchangeName, exchangeType, false)

        bindings.forEach { channel.queueBind(it.queueName, exchangeName, it.routingKey) }
    }


    fun produce(msg: String, routingKey: String) {
        channel.basicPublish(exchangeName, routingKey, null, msg.toByteArray())
    }

    override fun close() {
        connection.close()
        // (channels are closed automatically with the connection)
    }

    data class Binding(val queueName: String, val routingKey: String)
}
