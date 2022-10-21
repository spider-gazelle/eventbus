require "json"
require "db"
require "pg"
require "./event"

class EventBus
  @db : DB::Database?

  private def dispatch(evt : DBEvent)
    event = enrich(evt)
    @handlers.each do |h|
      spawn { h.on_event(event) }
    end
  end

  private def dispatch(evt : LifeCycleEvent)
    case evt
    in .start?   then @handlers.each { |h| spawn { h.on_start } }
    in .connect? then @handlers.each { |h| spawn { h.on_connect } }
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

    def initialize(@url : String, *channel : String)
      @channels = channel
    end

    def on_event(handler : DBEvent ->)
      @handler = handler
    end

    def start(h : Proc? = nil)
      @listener ||= ::PG.connect_listen(@url, @channels, &->event_handler(PQ::Notification))
      h.try &.call
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
