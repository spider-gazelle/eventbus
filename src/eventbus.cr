require "./eventbus/*"

class EventBus
  # :nodoc:
  alias ErrHandlerType = Exception | IO::Error

  @on_error : (ErrHandlerType ->)?

  def initialize(@url : String, **options)
    @handlers = Array(EventHandler).new
    @shutdown = Channel(Nil).new
    @blocking = false
    @on_error = nil
    @retry_count = options.fetch("retry_attempts", 0)
    @retry_interval = options.fetch("retry_interval", 5)
    @watchdog_interval = options.fetch("watchdog_interval", 5)
    @timeout = options.fetch("timeout", 5)
    @listener = PGListener.new(@url, PG::CHANNEL, retry_count: @retry_count,
      error_handler: ->error_handler(ErrHandlerType),
      set_count: ->(attempts : Int32) : Nil { @retry_attempt = attempts },
      watchdog_interval: @watchdog_interval,
      timeout: @timeout
    )
  end

  def self.new(url : URI, *handler : EventHandler)
    new(url).add_handler(*handler)
  end

  def add_handler(*handler : EventHandler)
    @handlers.concat(handler.to_a)
    self
  end

  def on_error(handler : ErrHandlerType ->)
    @on_error = handler
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

  def disable_cdc_for(table : String) : Nil
    PG.disable_cdc_for(@url, table)
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
    close_db
  end
end
