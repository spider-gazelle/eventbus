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
    size = 200
    spawn do
      1.upto(size) { |idx| insert_rec(idx) }
    end
    redis = Redis.new(url: REDIS_URL)
    count = 0
    redis.subscribe("public.spec_test.cdc_events") do |on|
      on.message do |_, message|
        count += 1
        evt = EventBus::Event.from_json(message)
        evt.schema.should eq("public")
        evt.table.should eq("spec_test")
        redis.unsubscribe("public.spec_test.cdc_events") if count == size
      end
    end
    count.should eq(size)
    eb.close
  end

  it "can receive change fields" do
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
    update_rec(1, "New Testing")
    evt = ch.receive
    evt.changes.should eq(%([{"new":"New Testing","old":"Testing","field":"name"}]))
    eb.close
  end
end
