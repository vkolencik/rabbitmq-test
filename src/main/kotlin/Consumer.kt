import com.github.ajalt.mordant.TermColors
import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import java.io.Closeable

@Suppress("CanBeParameter")
open class Consumer(private val queueName: String, private val consumerName: String? = null) : Closeable {
    companion object {
        private var nextConsumerId = 1
        private var nextConsumerColorIndex = 0

        private val colors = with (TermColors()) {
            arrayOf(
                brightBlue,
                brightRed,
                brightGreen,
                yellow,
                brightMagenta)
        }
    }

    private val connection = ConnectionFactory().newConnection()

    @Suppress("MemberVisibilityCanBePrivate")
    protected val channel: Channel = connection.createChannel()

    private val consumerId = nextConsumerId++
    private val color = colors[nextConsumerColorIndex++ % colors.size]

    init {
        channel.queueDeclare(queueName, false, false, false, emptyMap())

        channel.basicConsume(
            queueName,
            false,
            DeliverCallback { _, message ->
                if (beforeConsume(message)) {

                    var consumerDescription = consumerId.toString()
                    if (consumerName != null)
                        consumerDescription += " - $consumerName"

                    println(color("Consumer $consumerDescription received message (${message.envelope.deliveryTag}): ${String(message.body)}"))
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
    @Suppress("MemberVisibilityCanBePrivate")
    protected fun beforeConsume(@Suppress("UNUSED_PARAMETER") message: Delivery?) = true

    override fun close() {
        connection.close()
        // (channels are closed automatically with the connection)
    }
}
