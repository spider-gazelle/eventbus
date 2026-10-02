require "./spec_helper"

POLICY_TABLE = "spec_policy"

private def policy_events
  query_scalar("SELECT count(*) FROM eventbus_cdc_events WHERE event_table = '#{POLICY_TABLE}'").as(Int64)
end

describe "per-table CDC update policy" do
  before_each do
    run_sql("DROP TABLE IF EXISTS #{POLICY_TABLE}")
    run_sql("CREATE TABLE #{POLICY_TABLE}(id int PRIMARY KEY, name text, heartbeat int, item text)")
    EventBus.new(PG_DATABASE_URL).ensure_cdc_for(POLICY_TABLE).should be_true
    run_sql("DELETE FROM eventbus_cdc_events WHERE event_table = '#{POLICY_TABLE}'")
  end

  after_each { run_sql("DROP TABLE IF EXISTS #{POLICY_TABLE}") }

  it "persists telemetry without events while preserving insert, mixed update and delete events" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat", "item"]).should be_true
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    policy_events.should eq(1)
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 1, item = 'b'")
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = heartbeat, item = NULL")
    query_scalar("SELECT heartbeat FROM #{POLICY_TABLE}").should eq(1)
    policy_events.should eq(1)
    run_sql("UPDATE #{POLICY_TABLE} SET name = 'b', heartbeat = 2")
    policy_events.should eq(2)
    run_sql("DELETE FROM #{POLICY_TABLE}")
    policy_events.should eq(3)
  end

  it "preserves policy on ordinary and bulk registration and repairs update trigger drift" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
    before = trigger_meta(POLICY_TABLE)
    eb.ensure_cdc_for(POLICY_TABLE).should be_true
    eb.ensure_cdc_for_all_tables.should be_true
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat", "heartbeat"]).should be_true
    trigger_meta(POLICY_TABLE).should eq(before)
    run_sql("DROP TRIGGER eventbus_notify_change_update ON #{POLICY_TABLE}")
    eb.ensure_cdc_for(POLICY_TABLE).should be_true
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 1")
    policy_events.should eq(1)
  end

  it "replaces the installed policy when a differing policy is declared" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat", "item"]).should be_true
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 1, item = 'b'")
    policy_events.should eq(1)

    # narrowing the policy restores events for the removed column
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
    run_sql("UPDATE #{POLICY_TABLE} SET item = 'c'")
    policy_events.should eq(2)
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 2")
    policy_events.should eq(2)

    # the replaced policy is preserved by ordinary registration
    eb.ensure_cdc_for(POLICY_TABLE).should be_true
    eb.ensure_cdc_for_all_tables.should be_true
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 3")
    policy_events.should eq(2)

    # an explicit empty list restores ordinary UPDATE events
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: [] of String).should be_true
    query_scalar("SELECT count(*) FROM pg_trigger WHERE tgrelid = '#{POLICY_TABLE}'::regclass AND tgname = 'eventbus_notify_change_update'").should eq(0)
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 4")
    policy_events.should eq(3)
  end

  it "validates columns before replacing the installed policy" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
    before = trigger_meta(POLICY_TABLE)
    expect_raises(ArgumentError) { eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat", "missing"]) }
    trigger_meta(POLICY_TABLE).should eq(before)
  end

  it "uses expected current policy for compare-and-swap reset" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
    expect_raises(ArgumentError) { eb.replace_cdc_update_policy(POLICY_TABLE, ignore_update_columns: [] of String, expected_ignore_update_columns: ["item"]) }
    eb.replace_cdc_update_policy(POLICY_TABLE, ignore_update_columns: [] of String, expected_ignore_update_columns: ["heartbeat"]).should be_true
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = heartbeat")
    policy_events.should eq(2)
  end

  it "rejects missing columns and row identity suppression" do
    eb = EventBus.new(PG_DATABASE_URL)
    expect_raises(ArgumentError) { eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["missing"]) }
    expect_raises(ArgumentError) { eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["id"]) }
  end
end

describe "CDC policy reconciliation" do
  before_each do
    run_sql("DROP TABLE IF EXISTS #{POLICY_TABLE}")
    run_sql("CREATE TABLE #{POLICY_TABLE}(id int PRIMARY KEY, name text, heartbeat int, item text)")
    EventBus.new(PG_DATABASE_URL).ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
    run_sql("DELETE FROM eventbus_cdc_events WHERE event_table = '#{POLICY_TABLE}'")
  end
  after_each { run_sql("DROP TABLE IF EXISTS #{POLICY_TABLE}") }

  it "repairs an old installer replacing the base trigger without producing duplicates" do
    eb = EventBus.new(PG_DATABASE_URL)
    run_sql("CREATE OR REPLACE TRIGGER eventbus_notify_change_event AFTER INSERT OR UPDATE OR DELETE ON #{POLICY_TABLE} FOR EACH ROW EXECUTE FUNCTION public.eventbus_notify_change()")
    eb.ensure_cdc_for(POLICY_TABLE).should be_true
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 1")
    run_sql("UPDATE #{POLICY_TABLE} SET name = 'b'")
    policy_events.should eq(2)
  end

  it "recovers from a missing base trigger using the update trigger metadata" do
    drop_trigger_raw(POLICY_TABLE)
    eb = EventBus.new(PG_DATABASE_URL)
    eb.ensure_cdc_for(POLICY_TABLE).should be_true
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 1")
    policy_events.should eq(1)
  end

  it "requires the expected policy even when the desired policy is already installed" do
    expect_raises(ArgumentError) do
      EventBus.new(PG_DATABASE_URL).replace_cdc_update_policy(POLICY_TABLE, ignore_update_columns: ["heartbeat"], expected_ignore_update_columns: ["item"])
    end
  end

  it "rejects renamed columns and does not silently lose suppression" do
    run_sql("ALTER TABLE #{POLICY_TABLE} RENAME COLUMN heartbeat TO pulse")
    expect_raises(ArgumentError) { EventBus.new(PG_DATABASE_URL).ensure_cdc_for(POLICY_TABLE) }
    EventBus.new(PG_DATABASE_URL).replace_cdc_update_policy(POLICY_TABLE, ignore_update_columns: ["pulse"], expected_ignore_update_columns: ["heartbeat"]).should be_true
  end

  it "preserves policy on normal disable and clears both triggers on force uninstall" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.disable_cdc_for(POLICY_TABLE)
    eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
    eb.disable_cdc_for(POLICY_TABLE, force: true)
    query_scalar("SELECT count(*) FROM pg_trigger WHERE tgrelid = '#{POLICY_TABLE}'::regclass AND NOT tgisinternal").should eq(0)
    eb.ensure_cdc_for(POLICY_TABLE).should be_true
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 1")
    policy_events.should eq(2)
  end

  it "performs no DDL for a configured table under a concurrent writer lock" do
    eb = EventBus.new(PG_DATABASE_URL)
    metadata_sql = "SELECT string_agg(oid::text || ':' || xmin::text, ',' ORDER BY tgname) FROM pg_trigger WHERE tgrelid = '#{POLICY_TABLE}'::regclass AND NOT tgisinternal"
    before = query_scalar(metadata_sql)
    blocker = BlockingTxn.new(POLICY_TABLE, "ROW EXCLUSIVE")
    begin
      started = Time.instant
      eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
      (Time.instant - started).should be < 2.seconds
      query_scalar(metadata_sql).should eq(before)
    ensure
      blocker.close
    end
  end

  it "preserves concurrent configuration writes and rolls back CDC with the transaction" do
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    results = Channel(Exception?).new(2)
    ["heartbeat = 1", "name = 'b'"].each do |assignment|
      spawn do
        run_sql("UPDATE #{POLICY_TABLE} SET #{assignment} WHERE id = 1")
        results.send(nil)
      rescue ex
        results.send(ex)
      end
    end
    2.times { results.receive.should be_nil }
    query_scalar("SELECT name FROM #{POLICY_TABLE}").should eq("b")
    query_scalar("SELECT heartbeat FROM #{POLICY_TABLE}").should eq(1)
    policy_events.should eq(2)
    conn = DB.connect(PG_DATABASE_URL)
    begin
      conn.exec("BEGIN")
      conn.exec("UPDATE #{POLICY_TABLE} SET name = 'rollback', heartbeat = 2")
      conn.exec("ROLLBACK")
      policy_events.should eq(2)
      query_scalar("SELECT name FROM #{POLICY_TABLE}").should eq("b")
    ensure
      conn.close
    end
  end

  it "serializes conflicting concurrent declarations into one consistent policy" do
    eb = EventBus.new(PG_DATABASE_URL)
    eb.replace_cdc_update_policy(POLICY_TABLE, ignore_update_columns: [] of String, expected_ignore_update_columns: ["heartbeat"]).should be_true
    results = Channel(Bool).new(2)
    ["heartbeat", "item"].each do |column|
      spawn do
        results.send(EventBus.new(PG_DATABASE_URL).ensure_cdc_for(POLICY_TABLE, ignore_update_columns: [column]))
      rescue ArgumentError
        results.send(false)
      end
    end
    [results.receive, results.receive].should eq([true, true])
    # both trigger comments agree, so ordinary registration accepts the winner
    eb.ensure_cdc_for(POLICY_TABLE).should be_true
    run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 'a', 0, NULL)")
    run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 1, item = 'b'")
    policy_events.should eq(2)
  end
end

describe "CDC policy safety" do
  it "installs and repairs the default trigger while readers hold ACCESS SHARE" do
    run_sql("CREATE TABLE #{POLICY_TABLE}(id int)")
    blocker = BlockingTxn.new(POLICY_TABLE, "ACCESS SHARE")
    begin
      eb = EventBus.new(PG_DATABASE_URL, lock_timeout: "300ms", ddl_attempts: 1)
      eb.ensure_cdc_for(POLICY_TABLE).should be_true
      run_sql("CREATE OR REPLACE TRIGGER eventbus_notify_change_event AFTER INSERT ON #{POLICY_TABLE} FOR EACH ROW EXECUTE FUNCTION public.eventbus_notify_change()")
      eb.ensure_cdc_for(POLICY_TABLE).should be_true
    ensure
      blocker.close
      run_sql("DROP TABLE #{POLICY_TABLE}")
    end
  end

  it "quotes schema, table and ignored column names safely" do
    run_sql(%(CREATE SCHEMA "policy.schema"))
    begin
      run_sql(%(CREATE TABLE "policy.schema"."Odd.Table" (id int, "tick'\\value" int, name text)))
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(%("policy.schema"."Odd.Table"), ignore_update_columns: ["tick'\\value"]).should be_true
      run_sql(%(INSERT INTO "policy.schema"."Odd.Table" VALUES (1, 0, 'a')))
      run_sql(%(UPDATE "policy.schema"."Odd.Table" SET "tick'\\value" = 1))
      query_scalar("SELECT count(*) FROM eventbus_cdc_events WHERE event_schema = 'policy.schema'").should eq(1)
      eb.ensure_cdc_for_all_tables.should be_true
    ensure
      run_sql(%(DROP SCHEMA "policy.schema" CASCADE))
    end
  end

  it "repairs either missing metadata copy before the other trigger disappears" do
    run_sql("CREATE TABLE #{POLICY_TABLE}(id int, heartbeat int)")
    begin
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
      run_sql("DELETE FROM eventbus_cdc_events WHERE event_table = '#{POLICY_TABLE}'")
      ["eventbus_notify_change_event", "eventbus_notify_change_update"].each do |name|
        other = name == "eventbus_notify_change_event" ? "eventbus_notify_change_update" : "eventbus_notify_change_event"
        run_sql("COMMENT ON TRIGGER #{name} ON #{POLICY_TABLE} IS NULL")
        eb.ensure_cdc_for(POLICY_TABLE).should be_true
        run_sql("DROP TRIGGER #{other} ON #{POLICY_TABLE}")
        eb.ensure_cdc_for(POLICY_TABLE).should be_true
        run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 0)")
        run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = heartbeat + 1")
        query_scalar("SELECT count(*) FROM eventbus_cdc_events WHERE event_table = '#{POLICY_TABLE}' AND event_action = 'update'").should eq(0)
        run_sql("DELETE FROM #{POLICY_TABLE}")
      end
    ensure
      run_sql("DROP TABLE #{POLICY_TABLE}")
    end
  end

  it "does not notify for suppressed updates, with a committed notification barrier" do
    run_sql("CREATE TABLE #{POLICY_TABLE}(id int, heartbeat int)")
    notifications = Channel(String).new(10)
    listener = PG::ListenConnection.new(PG_DATABASE_URL, ["cdc_events"]) { |notification| notifications.send(notification.payload) }
    begin
      eb = EventBus.new(PG_DATABASE_URL)
      eb.ensure_cdc_for(POLICY_TABLE, ignore_update_columns: ["heartbeat"]).should be_true
      run_sql("INSERT INTO #{POLICY_TABLE} VALUES (1, 0)")
      select
      when message = notifications.receive
        JSON.parse(message)["action"].as_s.should eq("insert")
      when timeout(5.seconds)
        fail "CDC insert notification did not arrive"
      end
      run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = 1")
      run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = NULL")
      run_sql("UPDATE #{POLICY_TABLE} SET heartbeat = heartbeat")
      run_sql("SELECT pg_notify('cdc_events', 'policy-barrier')")
      select
      when message = notifications.receive
        message.should eq("policy-barrier")
      when timeout(5.seconds)
        fail "CDC notification barrier did not arrive"
      end
    ensure
      listener.close
      run_sql("DROP TABLE #{POLICY_TABLE}")
    end
  end
end
