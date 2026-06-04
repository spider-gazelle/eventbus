require "./eventbus/*"

class EventBus
  # :nodoc:
  alias ErrHandlerType = Exception | IO::Error
  PARALLEL_JOBS = (ENV["PARALLEL_JOBS"]? || ENV["PARALELL_JOBS"]? || 1).to_i

  @on_error : (ErrHandlerType ->)?
  @lock_timeout : String
  @ddl_attempts : Int32
  @ddl_backoff_ms : Int32

  def initialize(@url : String, **options)
    @handlers = Array(EventHandler).new
    @shutdown = Channel(Nil).new
    @blocking = false
    @on_error = nil
    @retry_count = options.fetch("retry_attempts", 0).to_i
    @retry_interval = options.fetch("retry_interval", 5).to_i
    @watchdog_interval = options.fetch("watchdog_interval", 5).to_i
    @timeout = options.fetch("timeout", 5).to_i
    @lock_timeout = options.fetch("lock_timeout", PG::LOCK_TIMEOUT).to_s
    @ddl_attempts = options.fetch("ddl_attempts", PG::DDL_ATTEMPTS).to_i
    @ddl_backoff_ms = options.fetch("ddl_backoff_ms", PG::DDL_BACKOFF_MS).to_i
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
    handler.each { |hnd| @handlers.delete(hnd) }
    self
  end

  def ensure_cdc_for_all_tables : Bool
    PG.ensure_cdc_for_all_tables(@url, @lock_timeout, @ddl_attempts, @ddl_backoff_ms)
  end

  def ensure_cdc_for(table : String) : Bool
    PG.ensure_cdc_for(@url, table, @lock_timeout, @ddl_attempts, @ddl_backoff_ms)
  end

  # No-op unless `force` is set: the CDC trigger is shared infrastructure that other
  # services rely on, and dropping it takes an ACCESS EXCLUSIVE lock that queues every
  # read on the table behind it. Pass `force: true` only to genuinely uninstall.
  def disable_cdc_for(table : String, force : Bool = false) : Nil
    PG.disable_cdc_for(@url, table, force, @lock_timeout, @ddl_attempts, @ddl_backoff_ms)
  end

  def start : Nil
    task_runner.start
    dispatch(:start)
    @listener.on_event(->on_event(DBEvent))
    @listener.start -> { dispatch(:connect) }
    @blocking = false
  end

  def run : Nil
    start
    @blocking = true
    @shutdown.receive
  end

  def close : Nil
    @listener.stop -> { dispatch(:close) }
    @shutdown.send(nil) if @blocking
  ensure
    close_db
  end
end
