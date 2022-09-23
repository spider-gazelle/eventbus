require "./spec_helper"
require "redis"

describe EventBus do
  it "can initialize listen/notify for all tables" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.ensure_cdc_for_all_tables.should be_true
  end

  it "can initialize listen/notify for table" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.ensure_cdc_for(TABLE).should be_true
  end

  it "lifecycle events are triggered properly" do
    ch = Channel(EventBus::Event).new
    eb = EventBus.new(PG_DATABASE_URL)
    sh = SpecHandler.new(ch)
    eb.add_handler sh
    eb.start
    insert_rec(1)
    ch.receive
    eb.close
    sleep 1
    sh.events.should eq(["on_start", "on_connect", "on_event", "on_close"])
  end

  it "can receive raw events" do
    ch = Channel(EventBus::Event).new
    eb = EventBus.new(PG_DATABASE_URL)
    eb.add_handler SpecHandler.new(ch)
    eb.start
    insert_rec(1)
    evt = ch.receive
    evt.schema.should eq("public")
    evt.table.should eq("spec_test")
    evt.id.should eq(1)
    evt.data.should eq(%({"id":1,"name":"Testing"}))
    eb.close
  end

  it "can add & subscribe to redis for notifications" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.add_handler SpecRedisPublisher.new(REDIS_URL)
    eb.start
    insert_rec(2)
    redis = Redis.new(url: REDIS_URL)
    redis.subscribe("public.spec_test.cdc_events") do |on|
      on.message do |_, message|
        evt = EventBus::Event.from_json(message)
        evt.schema.should eq("public")
        evt.table.should eq("spec_test")
        evt.id.should eq(2)
        evt.data.should eq(%({"id":2,"name":"Testing"}))
        redis.unsubscribe("public.spec_test.cdc_events")
      end
    end
    eb.close
  end
end
