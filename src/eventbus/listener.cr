require "log"
require "json"
require "db"
require "pg"
require "../ext/pg"
require "./event"

# :nodoc:
module EventBusLogger
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

class EventBus
  include ::EventBusLogger

  @db : DB::Database?
  @retry_attempt : Int32 = 0
  @retry_interval : Int32
  @retry_count : Int32

  private def error_handler(err : ErrHandlerType)
    @retry_attempt += 1
    if @retry_attempt <= @retry_count
      Log.warn { "Received error '#{err.message}'. Disconnected from database, retrying attempt ##{@retry_attempt} after #{@retry_interval} seconds" }
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
    event = enrich(evt)
    @handlers.each do |h|
      spawn { h.on_event(event) }
    end
  end

  private def dispatch(evt : LifeCycleEvent)
    case evt
    in .start?   then @handlers.each { |h| spawn { h.on_start } }
    in .connect? then @handlers.each { |h| spawn { @retry_attempt = 0; h.on_connect } }
    in .close?   then @handlers.each { |h| spawn { h.on_close } }
    end
  end

  private def enrich(evt : DBEvent)
    Event.new(evt.timestamp, evt.schema, evt.table, evt.action, evt.id, fetch(evt))
  end

  private def on_event(event : DBEvent)
    dispatch(event)
  end

  private def connection
    (@db ||= DB.open(@url)).not_nil!
  end

  private def fetch(evt : DBEvent) : String
    connection.query_one "select event_data from public.eventbus_cdc_events where id = $1", evt.logid, &.read(JSON::Any).to_json
  end

  private enum LifeCycleEvent
    Start
    Connect
    Close
  end

  private class PGListener
    @listener : ::PG::ListenConnection?
    @handler : (DBEvent ->)?
    @channels : Enumerable(String)
    @error_handler : (Exception ->)?

    def initialize(@url : String, *channel : String, @error_handler = nil)
      @channels = channel
    end

    def on_event(handler : DBEvent ->)
      @handler = handler
    end

    def start(h : Proc? = nil)
      @listener || begin
        spawn do
          @listener = ::PG.connect_listen(@url, @channels, true, &->event_handler(PQ::Notification))
          h.try &.call
          @listener.try &.start
        rescue ex
          @error_handler.try &.call(ex)
          @listener = nil
        end
      end
    end

    def stop(h : Proc? = nil)
      @listener.try &.close
      h.try &.call
    end

    private def event_handler(event : ::PQ::Notification)
      @handler.try &.call(DBEvent.from_json(event.payload))
    end
  end

  private record DBEvent, logid : Int64, timestamp : Time, schema : String, table : String, action : Action, id : JSON::Any do
    include JSON::Serializable
  end
end
