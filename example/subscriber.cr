require "redis"
require "json"

channel = ENV["CHANNEL"]

redis = Redis.new(url: ENV["REDIS_URL"])
puts "Subscribed to channel '#{channel}'\n\n"
redis.subscribe(channel) do |msg|
  msg.message do |chn, message|
    puts "Received message on channel: #{chn}"
    puts JSON.parse(message).to_pretty_json
    redis.ping
  end
end
