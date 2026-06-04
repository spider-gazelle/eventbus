require "spec"
require "../src/eventbus"

PG_DATABASE_URL = ENV["PG_DATABASE_URL"]
REDIS_URL       = ENV["REDIS_URL"]
TABLE           = "spec_test"

Spec.before_suite { run_sql("CREATE TABLE IF NOT EXISTS #{TABLE}(id int, name VARCHAR(100))") }
Spec.after_suite { run_sql("drop table if exists #{TABLE}") }
Spec.before_each { run_sql("delete from #{TABLE}") }

def run_sql(cmd)
  DB.open(PG_DATABASE_URL) do |dbc|
    dbc.exec(cmd)
  end
end

def query_scalar(cmd)
  DB.open(PG_DATABASE_URL) do |dbc|
    dbc.scalar(cmd)
  end
end

# Identity of the CDC trigger's pg_trigger row: (oid, xmin) changes whenever the
# trigger is dropped/recreated or replaced, so an unchanged pair proves no DDL ran.
def trigger_meta(table = TABLE)
  DB.open(PG_DATABASE_URL) do |dbc|
    dbc.query_one?(
      "SELECT t.oid::int8, t.xmin::text FROM pg_trigger t " \
      "WHERE t.tgname = 'eventbus_notify_change_event' AND NOT t.tgisinternal " \
      "AND t.tgrelid = ('public.' || quote_ident($1))::regclass",
      args: [table], as: {Int64, String})
  end
end

def drop_trigger_raw(table = TABLE)
  run_sql(%(DROP TRIGGER IF EXISTS eventbus_notify_change_event ON "#{table}"))
end

# Installs a deliberately divergent trigger (INSERT only) for the drift-repair spec.
def make_wrong_trigger(table = TABLE)
  drop_trigger_raw(table)
  run_sql(
    %(CREATE TRIGGER eventbus_notify_change_event AFTER INSERT ON "#{table}" ) \
    %(FOR EACH ROW EXECUTE FUNCTION public.eventbus_notify_change()))
end

def insert_event_row(age : Time::Span, marker : String)
  run_sql(
    "INSERT INTO eventbus_cdc_events(event_schema, event_table, event_action, row_id, event_data, created_at) " \
    "VALUES ('public', '#{TABLE}', 'insert', '#{marker}', '{}'::jsonb, " \
    "CURRENT_TIMESTAMP - INTERVAL '#{age.total_seconds.to_i} seconds')")
end

def event_row_exists?(marker : String) : Bool
  query_scalar("SELECT EXISTS(SELECT 1 FROM eventbus_cdc_events WHERE row_id = '#{marker}')").as(Bool)
end

# Holds an open transaction with an explicit table lock on a dedicated connection,
# so specs can simulate concurrent reader/writer contention.
class BlockingTxn
  @conn : DB::Connection

  def initialize(table : String, mode : String)
    @conn = DB.connect(PG_DATABASE_URL)
    @conn.exec("BEGIN")
    @conn.exec(%(LOCK TABLE "#{table}" IN #{mode} MODE))
  end

  def close
    @conn.exec("ROLLBACK") rescue nil
    @conn.close rescue nil
  end
end

def insert_rec(id : Int)
  run_sql("insert into #{TABLE} values(#{id}, 'Testing')")
end

def update_rec(id : Int, value : String)
  run_sql("update #{TABLE} set name = '#{value}' where id = #{id}")
end

class SpecHandler < EventBus::EventHandler
  getter events : Array(String)

  def initialize(@ch : Channel(EventBus::Event)? = nil)
    @events = Array(String).new
  end

  def on_event(event : EventBus::Event) : Nil
    @events << "on_event"
    @ch.try &.send(event)
  end

  def on_start : Nil
    @events << "on_start"
  end

  def on_connect : Nil
    @events << "on_connect"
  end

  def on_close : Nil
    @events << "on_close"
  end
end

class SpecRedisPublisher < EventBus::EventHandler
  def initialize(url : String)
    @redis = Redis.new(url: url)
  end

  def on_event(event : EventBus::Event) : Nil
    channel = "#{event.schema}.#{event.table}.cdc_events"
    @redis.publish(channel, event.to_json)
  end

  def on_close
    @redis.close
  end
end
