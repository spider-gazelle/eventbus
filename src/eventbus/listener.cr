require "log"
require "json"
require "db"
require "pg"
require "time"
require "uri"
require "uri/params"
require "../ext/pg"
require "./event"

# :nodoc:
private module EventBusLogger
  ::Log.progname = "EventBus"
  log_backend = ::Log::IOBackend.new
  log_level = ::Log::Severity::Info

  builder = ::Log.builder
  builder.bind "*", log_level, log_backend

  ::Log.setup_from_env(
    default_level: log_level,
    builder: builder,
    backend: log_backend,
    log_level_env: "LOG_LEVEL",
  )
end

# :nodoc:
private module EventBusDBFuncs
  @@db : DB::Database?

  private def pool
    (@@db ||= DB.open(@url)).not_nil!
  end

  private def connection
    pool.using_connection { |_db| yield _db }
  end

  private def close_db
    @@db.try &.close
    @@db = nil
  end
end

class EventBus
  include EventBusLogger
  include EventBusDBFuncs

  # @db : DB::Database?
  @retry_attempt : Int32 = 0
  @retry_interval : Int32
  @retry_count : Int32
  @watchdog_interval : Int32
  @timeout : Int32

  private def error_handler(err : ErrHandlerType)
    @retry_attempt += 1
    if (@retry_count <= 0) || (@retry_attempt <= @retry_count)
      Log.warn { "Received error '#{err.message || err.class.name}'. Disconnected from database, retrying attempt ##{@retry_attempt} after #{@retry_interval} seconds" }
      @listener.set_attempts(@retry_attempt)
      sleep(@retry_interval)
      @listener.start ->{ dispatch(:connect) }
    else
      Log.error(exception: err) { "Giving up after attempting #{@retry_count} retries to re-connect to database." }
      if (on_error = @on_error)
        on_error.call(err)
      else
        raise err
      end
    end
  end

  private def dispatch(evt : DBEvent)
    if event = enrich(evt)
      @handlers.each do |h|
        spawn { h.on_event(event) }
      end
    else
      Log.error { "Unable to dispatch event to listeners due to problem reading the event details for id #{evt.logid}" }
    end
  end

  private def dispatch(evt : LifeCycleEvent)
    case evt
    in .start?   then @handlers.each { |h| spawn { h.on_start } }
    in .connect? then @handlers.each { |h| spawn { @retry_attempt = 0; @listener.set_attempts(0); h.on_connect } }
    in .close?   then @handlers.each { |h| spawn { h.on_close } }
    end
  end

  private def enrich(evt : DBEvent)
    data = fetch(evt)
    if data
      Event.new(evt.timestamp, evt.schema, evt.table, evt.action, evt.id, *data)
    end
  end

  private def on_event(event : DBEvent)
    spawn { dispatch(event) }
  end

  private def fetch(evt : DBEvent) : Tuple(String, String?)?
    attempt = 0
    while (@retry_count <= 0 || attempt <= @retry_count)
      begin
        attempt += 1
        res = connection(&.query_one "select event_data, change_data from public.eventbus_cdc_events where id = $1", evt.logid, as: {JSON::Any, JSON::Any?})
        return {res[0].to_json, res[1].try &.to_json}
      rescue err
        Log.warn { "Fetching record id: #{evt.logid} from DB. Received error '#{err.message || err.class.name}'. retrying attempt ##{attempt} after #{@retry_interval} seconds" }
        sleep(@retry_interval)
      end
    end
    Log.error { "Giving up after attempting #{@retry_count} retries to re-connect to database and fetch record id: #{evt.logid}." }
    nil
  end

  private enum LifeCycleEvent
    Start
    Connect
    Close
  end

  private class PGListener
    include EventBusLogger

    @listener : ::PG::ListenConnection?
    @handler : (DBEvent ->)?
    @channels : Enumerable(String)
    # @watch_dog : WatchDog

    HEARTBEAT_CHANNEL = "eventbus_heartbeat"

    def initialize(@url : String, *channel : String, @retry_count : Int32, @error_handler : Exception -> Nil, @set_count : Int32 -> Nil, watchdog_interval = 5, timeout = 5)
      @running = false
      @retry_attempt = 0
      @channels = channel.to_a.unshift(HEARTBEAT_CHANNEL)
      @watch_dog = WatchDog.new(@url, HEARTBEAT_CHANNEL, ->error_handler(Exception), watchdog_interval, timeout)
      @watch_dog.run
    end

    def on_event(handler : DBEvent ->)
      @handler = handler
    end

    def start(h : Proc? = nil)
      return if @running
      spawn do
        @listener = ::PG.connect_listen(@url, @channels, true, &->event_handler(PQ::Notification))
        h.try &.call
        @running = true
        @watch_dog.run
        @listener.try &.start
      rescue ex
        @running = false
        @listener.try &.close rescue nil
        @listener = nil
        @watch_dog.cancel
        @error_handler.call(ex)
      end
      Fiber.yield
    end

    def stop(h : Proc? = nil)
      @running = false
      @listener.try &.close
      @watch_dog.stop
      h.try &.call
    end

    def set_attempts(attempts : Int32)
      @retry_attempt = attempts
    end

    private def event_handler(event : ::PQ::Notification)
      if event.channel == HEARTBEAT_CHANNEL
        @watch_dog.run
        set_attempts(0)
        return
      end
      @handler.try &.call(DBEvent.from_json(event.payload))
    end

    private def error_handler(ex : Exception) : Nil
      cause = ex.cause.nil? ? "" : " due to #{ex.cause.try &.class.name}"
      @retry_attempt += 1
      @set_count.call(@retry_attempt)
      if (@retry_count <= 0) || (@retry_attempt <= @retry_count)
        Log.warn { "Watchdog: detected database connection problem: #{ex.class.name}" + cause + ". Retrying attempt ##{@retry_attempt} after #{@watch_dog.interval} seconds" }
        @watch_dog.run
        start
      else
        @error_handler.call(ex)
      end
    end

    private class WatchDog
      include EventBusDBFuncs
      @url : String
      getter interval : Int32
      @timer : Timer?

      def initialize(url : String, @channel : String, @error_handler : Exception -> Nil, @interval : Int32 = 5, @timeout : Int32 = 5)
        @url = check_timeout(url, @timeout)
        @running = false
        @stopped = false
      end

      def run : Nil
        return if @running || @stopped
        @running = true
        @timer = Timer.new(@interval.seconds) {
          begin
            @running = false
            connection do |db|
              db.exec "SELECT pg_notify($1, $2)", @channel, true
            end
          rescue ex
            spawn { @error_handler.call(ex) }
          end
        }
      end

      def stop
        @stopped = true
        close_db
      end

      def cancel
        return unless @running
        @running = false
        @timer.try &.cancel
      end

      def interval
        @interval + @timeout
      end

      private def check_timeout(url, timeout)
        uri = URI.parse(url)
        if q = uri.query
          params = URI::Params.parse(q)
          unless params["timeout"]?
            params.add("timeout", timeout.to_s)
          end
          uri.query = params.to_s
          uri.to_s
        else
          "#{url}?timeout=#{timeout}"
        end
      end
    end

    private class Timer
      def initialize(@when : Time, &block)
        @channel = Channel(Nil).new(1)
        @completed = false
        @cancelled = false

        spawn do
          loop do
            sleep({Time::Span.zero, @when - Time.utc}.max)
            break if (@completed || @cancelled)
            next if Time.utc < @when
            break @channel.send(nil)
          end
        end

        spawn do
          @channel.receive

          unless @cancelled
            @completed = true
            block.call
          end
        end
      end

      def self.new(when : Time::Span, &block)
        new(Time.utc + when, &block)
      end

      def cancel
        return if @completed || @cancelled
        @cancelled = true
        @channel.send(nil)
      end
    end
  end

  private record DBEvent, logid : Int64, timestamp : Time, schema : String, table : String, action : Action, id : JSON::Any do
    include JSON::Serializable
  end
end
