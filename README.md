# nifi-rabbitmq-bundle

This project is in active development.  It is not ready for production use.

This processor only supports direct exchanges.  Once the code is tested, support for other exchange types will be added.

[RabbitMQ](https://www.rabbitmq.com) processor for [Apache NIFI](https://nifi.apache.org).

## Compile

`mvn clean install`

Copy `nifi-rabbitmq-nar/target/nifi-rabbitmq-nar-0.3.0-SNAPSHOT.nar` to `$NIFI_HOME/lib/` and restart NIFI.

## Testing

`mvn test`

## License

Apache 2.0

#### Example: Sending messages to nifi-rabbitmq-bundle using Ruby

```ruby
require 'bunny'
require 'faker'

conn = Bunny.new
conn.start

ch = conn.create_channel
q = ch.queue("hello", :durable => true)

1.upto(100) do
  msg = Faker::Lorem.paragraphs.join
  ch.default_exchange.publish(msg, :routing_key => q.name)
  puts " [x] Sent #{msg}"
end

conn.close
```
