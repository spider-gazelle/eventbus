require "./spec_helper"

describe "CDC trigger management" do
  describe "ensure_cdc_for" do
    it "is idempotent: a second call performs no DDL" do
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(TABLE).should be_true
      before = trigger_meta
      before.should_not be_nil
      eb.ensure_cdc_for(TABLE).should be_true
      trigger_meta.should eq(before)
    end

    it "returns instantly when the trigger is installed, even under a concurrent writer lock" do
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(TABLE).should be_true
      blocker = BlockingTxn.new(TABLE, "ROW EXCLUSIVE")
      begin
        started = Time.instant
        eb.ensure_cdc_for(TABLE).should be_true
        # a blocked install would take >10s (5 attempts x 2s lock_timeout + backoff)
        (Time.instant - started).should be < 2.seconds
      ensure
        blocker.close
      end
    end

    it "hits lock_timeout, retries and fails gracefully when installation is blocked" do
      eb = EventBus.new(PG_DATABASE_URL, lock_timeout: "300ms", ddl_attempts: 2, ddl_backoff_ms: 50)
      eb.ensure_cdc_for(TABLE).should be_true
      drop_trigger_raw
      blocker = BlockingTxn.new(TABLE, "EXCLUSIVE")
      begin
        started = Time.instant
        eb.ensure_cdc_for(TABLE).should be_false
        (Time.instant - started).should be < 5.seconds
      ensure
        blocker.close
      end
      eb.ensure_cdc_for(TABLE).should be_true
    end

    it "repairs a drifted trigger definition" do
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(TABLE).should be_true
      correct = trigger_meta
      make_wrong_trigger
      trigger_meta.should_not eq(correct)
      eb.ensure_cdc_for(TABLE).should be_true
      update_meta = trigger_meta
      update_meta.should_not be_nil
      # and it now matches the canonical definition: a further call performs no DDL
      eb.ensure_cdc_for(TABLE).should be_true
      trigger_meta.should eq(update_meta)
    end

    it "returns false for a table that does not exist" do
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for("no_such_table_here").should be_false
    end

    it "adopts triggers installed by previous library versions without performing DDL" do
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(TABLE).should be_true
      # recreate the trigger exactly as eventbus 1.x did: DROP + CREATE, EXECUTE PROCEDURE
      drop_trigger_raw
      run_sql(
        %(CREATE TRIGGER eventbus_notify_change_event AFTER INSERT OR UPDATE OR DELETE ON "#{TABLE}" ) \
        %(FOR EACH ROW EXECUTE PROCEDURE public.eventbus_notify_change()))
      legacy = trigger_meta
      legacy.should_not be_nil
      eb.ensure_cdc_for(TABLE).should be_true
      # catalog row untouched: the legacy trigger was recognised as canonical
      trigger_meta.should eq(legacy)
    end
  end

  describe "ensure_cdc_for_all_tables" do
    it "is idempotent and replaces the legacy plpgsql loop" do
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for_all_tables.should be_true
      before = trigger_meta
      eb.ensure_cdc_for_all_tables.should be_true
      trigger_meta.should eq(before)
      query_scalar("SELECT to_regprocedure('public.eventbus_cdc_for_all_tables()')::text").should be_nil
    end
  end

  describe "disable_cdc_for" do
    it "is a no-op by default: the trigger is shared infrastructure" do
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(TABLE).should be_true
      eb.disable_cdc_for(TABLE)
      trigger_meta.should_not be_nil
    end

    it "removes the trigger when forced" do
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(TABLE).should be_true
      eb.disable_cdc_for(TABLE, force: true)
      trigger_meta.should be_nil
      eb.ensure_cdc_for(TABLE).should be_true
    end
  end

  describe "cleanup" do
    it "creates an index on created_at" do
      EventBus.new(PG_DATABASE_URL).ensure_cdc_for(TABLE).should be_true
      query_scalar(
        "SELECT count(*) FROM pg_indexes WHERE indexname = 'eventbus_cdc_events_created_at_idx'"
      ).should eq(1)
    end

    it "eventbus_cdc_run_cleanup removes only rows older than the retention period" do
      EventBus.new(PG_DATABASE_URL).ensure_cdc_for(TABLE).should be_true
      insert_event_row(2.days, "cleanup-spec-old")
      insert_event_row(1.hour, "cleanup-spec-fresh")
      run_sql("SELECT public.eventbus_cdc_run_cleanup()")
      event_row_exists?("cleanup-spec-old").should be_false
      event_row_exists?("cleanup-spec-fresh").should be_true
    end
  end

  describe "event fetch" do
    it "does not wedge the worker when an event references a missing row" do
      ch = Channel(EventBus::Event).new
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(TABLE).should be_true
      eb.add_handler SpecHandler.new(ch)
      eb.start
      bogus = {logid: 999_999_999_i64, timestamp: Time.utc, schema: "public", table: TABLE, action: "insert", id: 42}.to_json
      run_sql("SELECT pg_notify('cdc_events', '#{bogus}')")
      insert_rec(1)
      select
      when evt = ch.receive
        evt.id.should eq(1)
      when timeout(10.seconds)
        fail "worker wedged: real event was not dispatched after a bogus logid"
      end
      eb.close
    end
  end
end
