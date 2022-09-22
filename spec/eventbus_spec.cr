require "./spec_helper"
require "redis"

describe EventBus do
  it "can initialize listen/notify plpgsql" do
    EventBusSchema.init.should be_true
  end

  it "can receive raw events" do
    ch = Channel(Event).new
    spawn { SpecHandler.new.start(ch) }
    insert_rec(1)
    evt = ch.receive
    evt.schema.should eq("public")
    evt.table.should eq("spec_test")
    evt.id.should eq(1)
    evt.data.should eq(%({"id":1,"name":"Testing"}))
  end

  it "can subscribe to redis for notifications" do
    spawn { SpecHandler.new.start }
    insert_rec(2)
    redis = Redis.new(url: App::REDIS_URL)
    redis.subscribe("public.spec_test.cdc_events") do |on|
      on.message do |_, message|
        evt = Event.from_json(message)
        evt.schema.should eq("public")
        evt.table.should eq("spec_test")
        evt.id.should eq(2)
        evt.data.should eq(%({"id":2,"name":"Testing"}))
        redis.unsubscribe("public.spec_test.cdc_events")
      end
    end
  end
end

class SpecHandler < EventHandler
  @ch : Channel(Event)?

  def initialize
    @app = Application.new
  end

  def start(ch = nil)
    @ch = ch
    @app.add_handler(self)

    @app.run
  end

  def on_event(event : Event) : Nil
    @ch.try &.send(event)
    @app.close
  end
end
