require "log"
require "../../src/eventbus"

class EventLogger < EventBus::EventHandler
  Log = ::Log.for("EventLogger")

  def on_event(event : EventBus::Event) : Nil
    Log.info { "Event: #{event.to_json}" }
  end

  def on_start : Nil
    Log.info { "on_start called" }
  end

  def on_connect : Nil
    Log.info { "on_connect called" }
  end

  def on_close : Nil
    Log.info { "on_close called" }
  end
end
