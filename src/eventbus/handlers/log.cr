require "../event"

module EventBus
  @[AutoWire]
  class EventLogger < EventHandler
    Log = App::Log.for("EventLogger")

    def on_event(event : Event) : Nil
      Log.info { "Event: #{event.to_json}" }
    end
  end
end
