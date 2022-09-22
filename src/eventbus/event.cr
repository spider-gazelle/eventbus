module EventBus
  annotation AutoWire; end

  abstract class EventHandler
    # Method invoked whenever there is new `Event` received.
    abstract def on_event(event : Event) : Nil

    # Method invoked when Application is going to start. Override this method, if you need to perform
    # some work
    def on_start : Nil
    end

    # Method invoked when Listener is connected to PG. Override this method, if you need to perform
    # some work
    def on_connect : Nil
    end

    # Method invoked when Application is going to close. Override this method, if you need to perform
    # some cleanups
    def on_close : Nil
    end
  end

  enum Action
    INSERT
    UPDATE
    DELETE

    def to_s(io : IO) : Nil
      io << self.to_s.downcase
    end
  end

  @[JSON::Serializable::Options(emit_nulls: true)]
  record Event, timestamp : Time, schema : String, table : String, action : Action, id : Int64, data : String? do
    include JSON::Serializable
  end
end
