import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import java.io.Closeable

@Suppress("CanBeParameter")
open class Consumer(private val queueName: String) : Closeable {
    companion object {
        var nextConsumerId = 1
    }

    private val connection = ConnectionFactory().newConnection()

    @Suppress("MemberVisibilityCanBePrivate")
    protected val channel: Channel = connection.createChannel()

    private val consumerId = Consumer.nextConsumerId++

    init {
        channel.queueDeclare(queueName, true, false, false, emptyMap())

        channel.basicConsume(
            queueName,
            false,
            DeliverCallback { _, message ->
                if (beforeConsume(message)) {
                    println("Consumer $consumerId received message (${message.envelope.deliveryTag}): ${String(message.body)}")
                    channel.basicAck(message.envelope.deliveryTag, false)
                }
            },
            CancelCallback {  }
        )
    }

    /**
     * Can be overridden and modify consume behaviour. Returning <code>false</code> will abort the receiving process
     * (not even NACK will be sent).
     */
    private fun beforeConsume(@Suppress("UNUSED_PARAMETER") message: Delivery?) = true

    override fun close() {
        connection.close()
        // (channels are closed automatically with the connection)
    }
}
