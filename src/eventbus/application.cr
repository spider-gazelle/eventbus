require "json"
require "./init"
require "./event"
require "./handlers/*"

module EventBus
  class Application
    Log = App::Log.for("application")
    @db : DB::Database?

    def initialize
      @handlers = Array(EventHandler).new
      {{EventHandler.all_subclasses.reject(&.annotations(EventBus::AutoWire).empty?)}}.each do |e|
        Log.info { "Initializing #{e.name} event handler" }
        @handlers << e.new
      end
      @listener = EventListener.new(App::PG_DATABASE_URL, App::CHANNEL)
    end

    def self.new(*handler : EventHandler)
      new.add_handler(*handler)
    end

    def add_handler(*handler : EventHandler)
      @handlers.concat(handler.to_a)
      self
    end

    def remove_handler(*handler : EventHandler)
      handler.each { |h| @handlers.delete(h) }
      self
    end

    def run
      dispatch(:start)
      @listener.on_event(->on_event(DBEvent))
      @listener.start ->{ dispatch(:connect) }
    end

    def close : Nil
      @listener.stop ->{ dispatch(:close) }
    ensure
      @db.try &.close
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
      (@db ||= DB.open(App::PG_DATABASE_URL)).not_nil!
    end

    private def fetch(evt : DBEvent) : String?
      return nil if evt.action == Action::DELETE
      connection.query_one? "select to_json(t) from #{evt.schema}.#{evt.table} t where t.id = #{evt.id}", &.read(JSON::Any).to_json
    end

    private enum LifeCycleEvent
      Start
      Connect
      Close
    end

    private class EventListener
      @listener : PG::ListenConnection?
      @handler : (DBEvent ->)?
      @channels : Enumerable(String)

      def initialize(@url : String, *channel : String)
        @channels = channel
        @shutdown = Channel(Nil).new
      end

      def on_event(handler : DBEvent ->)
        @handler = handler
      end

      def start(h : Proc? = nil)
        @listener ||= PG.connect_listen(@url, @channels, &->event_handler(PQ::Notification))
        h.try &.call
        @shutdown.receive
      end

      def stop(h : Proc? = nil)
        @listener.try &.close
        h.try &.call
        @shutdown.send(nil)
      end

      private def event_handler(event : PQ::Notification)
        @handler.try &.call(DBEvent.from_json(event.payload))
      end
    end

    private record DBEvent, timestamp : Time, schema : String, table : String, action : Action, id : Int64 do
      include JSON::Serializable
    end
  end
end
