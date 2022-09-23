require "./eventbus/*"

class EventBus
  def initialize(@url : String)
    @handlers = Array(EventHandler).new
    @shutdown = Channel(Nil).new
    @listener = PGListener.new(@url, PG::CHANNEL)
    @blocking = false
  end

  def self.new(url : URI, *handler : EventHandler)
    new(url).add_handler(*handler)
  end

  def add_handler(*handler : EventHandler)
    @handlers.concat(handler.to_a)
    self
  end

  def remove_handler(*handler : EventHandler)
    handler.each { |h| @handlers.delete(h) }
    self
  end

  def ensure_cdc_for_all_tables : Bool
    PG.ensure_cdc_for_all_tables(@url)
  end

  def ensure_cdc_for(table : String) : Bool
    PG.ensure_cdc_for(@url, table)
  end

  def start : Nil
    dispatch(:start)
    @listener.on_event(->on_event(DBEvent))
    @listener.start ->{ dispatch(:connect) }
    @blocking = false
  end

  def run : Nil
    start
    @blocking = true
    @shutdown.receive
  end

  def close : Nil
    @listener.stop ->{ dispatch(:close) }
    @shutdown.send(nil) if @blocking
  ensure
    @db.try &.close
  end
end
