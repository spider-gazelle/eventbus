require "log"
require "../src/eventbus"
require "./handlers/*"

# Log = ::Log.for("DemoApp")
LOG_BACKEND     = ::Log::IOBackend.new
PG_DATABASE_URL = ENV["PG_DATABASE_URL"]
REDIS_URL       = ENV["REDIS_URL"]

puts "Starting EventBus Application"

evtbus = EventBus.new(PG_DATABASE_URL)

unless evtbus.ensure_cdc_for_all_tables
  puts "Unable to initialize schema. Terminating..."
  exit(1)
end

evtbus.add_handler(EventLogger.new, RedisPublisher.new(REDIS_URL))

terminate = Proc(Signal, Nil).new do |signal|
  puts " > terminating gracefully"
  spawn { evtbus.close }
  signal.ignore
end

# Detect ctr-c to shutdown gracefully
# Docker containers use the term signal
Signal::INT.trap &terminate
Signal::TERM.trap &terminate

# Allow signals to change the log level at run-time
logging = Proc(Signal, Nil).new do |signal|
  level = signal.usr1? ? Log::Severity::Debug : Log::Severity::Info
  puts " > Log level changed to #{level}"
  Log.builder.bind "DemoApp.*", level, LOG_BACKEND
  signal.ignore
end

# Turn on DEBUG level logging `kill -s USR1 %PID`
# Default production log levels (INFO and above) `kill -s USR2 %PID`
Signal::USR1.trap &logging
Signal::USR2.trap &logging

# Start the EventBus
evtbus.run
