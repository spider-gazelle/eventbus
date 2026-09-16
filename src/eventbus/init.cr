require "db"
require "log"
require "pg"

class EventBus
  private module PG
    Log     = ::Log.for("PGInitializer")
    CHANNEL = "cdc_events"

    TRIGGER_NAME        = "eventbus_notify_change_event"
    UPDATE_TRIGGER_NAME = "eventbus_notify_change_update"
    POLICY_PREFIX       = "eventbus:update-policy:v1:"
    TRIGGER_PROC        = "public.eventbus_notify_change()"

    # DDL safety defaults, overridable via environment or `EventBus.new` options.
    LOCK_TIMEOUT   = ENV["EVENTBUS_LOCK_TIMEOUT"]? || "2s"
    DDL_ATTEMPTS   = (ENV["EVENTBUS_DDL_ATTEMPTS"]? || "5").to_i
    DDL_BACKOFF_MS = (ENV["EVENTBUS_DDL_BACKOFF_MS"]? || "100").to_i

    # Validated before interpolation into function bodies (cannot be bind parameters there).
    RETENTION = (ENV["EVENTBUS_RETENTION"]? || "1 day").tap do |value|
      raise ArgumentError.new("EVENTBUS_RETENTION must look like '1 day', '12 hours' or '30 minutes'") unless value.matches?(/\A\d+ (days?|hours?|minutes?)\z/)
    end
    CLEANUP_PROBABILITY = (ENV["EVENTBUS_CLEANUP_PROBABILITY"]? || "0.01").to_f.tap do |value|
      raise ArgumentError.new("EVENTBUS_CLEANUP_PROBABILITY must be within 0..1") unless (0.0..1.0).includes?(value)
    end

    # Advisory lock identifiers: the original global key guards schema setup, the second
    # namespaces per-table trigger DDL so installs of different tables don't serialize.
    SETUP_LOCK_KEY     = 2234516474639426746_i64
    TRIGGER_LOCK_SPACE =          1122334455_i32

    SQLSTATE_LOCK_NOT_AVAILABLE = "55P03"
    SQLSTATE_UNDEFINED_TABLE    = "42P01"

    # Setup is memoized per database url: schema objects persist, so re-running the
    # statements on every subscription is wasted round-trips and catalog churn.
    @@setup_done = Set(String).new
    @@setup_mutex = Mutex.new

    def self.ensure_cdc_for_all_tables(url, lock_timeout : String = LOCK_TIMEOUT, attempts : Int32 = DDL_ATTEMPTS, backoff_ms : Int32 = DDL_BACKOFF_MS) : Bool
      with_ddl_connection(url) do |conn|
        Log.info { "Initializing DB Schema" }
        setup_eventbus(conn, url)
        # Each table is installed in its own short transaction so a single contended
        # table cannot accumulate locks across the rest of the schema.
        results = list_cdc_tables(conn).map do |table|
          install_trigger(conn, table, lock_timeout, attempts, backoff_ms)
        end
        Log.info { "DB Schema initialization completed" }
        results.all?
      end
    end

    def self.ensure_cdc_for(url, table, lock_timeout : String = LOCK_TIMEOUT, attempts : Int32 = DDL_ATTEMPTS, backoff_ms : Int32 = DDL_BACKOFF_MS, ignore_update_columns : Array(String)? = nil, expected_ignore_update_columns : Array(String)? = nil) : Bool
      qualified = QualifiedTable.parse(table)
      with_ddl_connection(url) do |conn|
        setup_eventbus(conn, url)
        install_trigger(conn, qualified, lock_timeout, attempts, backoff_ms, ignore_update_columns, expected_ignore_update_columns)
      end
    end

    # Leaves the trigger in place unless `force` is set: it is shared infrastructure that
    # other services rely on, and dropping it takes an ACCESS EXCLUSIVE lock that queues
    # every read on the table behind it. Pass `force: true` only for genuine uninstalls.
    def self.disable_cdc_for(url, table, force : Bool = false, lock_timeout : String = LOCK_TIMEOUT, attempts : Int32 = DDL_ATTEMPTS, backoff_ms : Int32 = DDL_BACKOFF_MS) : Nil
      return unless force
      qualified = QualifiedTable.parse(table)
      with_ddl_connection(url) do |conn|
        drop_trigger(conn, qualified, lock_timeout, attempts, backoff_ms)
      end
    rescue ex
      # preserve the historical contract that disabling never raises
      Log.error(exception: ex) { "disable_cdc_for #{table}: failed to drop trigger" }
    end

    # Installs the CDC trigger idempotently. The catalog pre-check takes no lock on the
    # subject table, so the steady-state boot path (trigger already correct) performs no
    # DDL and acquires no table locks at all. When (re)installation is needed,
    # CREATE OR REPLACE TRIGGER only takes SHARE ROW EXCLUSIVE — readers are never
    # blocked — and lock_timeout with bounded retries stops writer contention queueing.
    # Resetting a policy also drops the UPDATE trigger, requiring ACCESS EXCLUSIVE.
    private def self.install_trigger(conn, table : QualifiedTable, lock_timeout, attempts, backoff_ms, requested : Array(String)? = nil, expected : Array(String)? = nil) : Bool
      with_ddl_retry("ensure_cdc_for", table, attempts, backoff_ms) do
        conn.transaction do |tx|
          db = tx.connection
          db.exec("SELECT set_config('lock_timeout', $1, true)", args: [lock_timeout])
          # taken across check + DDL so a concurrent force-disable cannot interleave;
          # keyed on the relation oid so the lock identity matches the catalog identity
          db.exec(ADVISORY_LOCK_SQL, args: [TRIGGER_LOCK_SPACE, table.quoted])
          metadata = installed_policy(db, table)
          current = metadata.try(&.columns) || [] of String
          desired = requested.try(&.uniq.sort!) || current
          check_policy(table, current, desired, requested, expected)
          validate_columns(db, table, desired)
          base_type = desired.empty? ? 29 : 13
          base_matches = trigger_matches?(db, table, TRIGGER_NAME, base_type, nil)
          update_matches = desired.empty? ? !trigger_exists?(db, table, UPDATE_TRIGGER_NAME) : trigger_matches?(db, table, UPDATE_TRIGGER_NAME, 17, metadata.try(&.condition))
          if current == desired && base_matches && update_matches
            restore_metadata(db, table, metadata) if metadata
            Log.debug { "CDC trigger already installed on #{table.quoted}" }
          else
            Log.info { "Installing CDC trigger on #{table.quoted}" }
            if desired.empty?
              db.exec(sprintf(CREATE_TRIGGER, table.quoted))
              db.exec("DROP TRIGGER IF EXISTS #{UPDATE_TRIGGER_NAME} ON #{table.quoted}") unless update_matches
              db.exec("COMMENT ON TRIGGER #{TRIGGER_NAME} ON #{table.quoted} IS NULL")
            else
              columns = desired.map { |column| sql_literal(column) }.join(", ")
              condition = "(to_jsonb(OLD) - ARRAY[#{columns}]::text[]) IS DISTINCT FROM (to_jsonb(NEW) - ARRAY[#{columns}]::text[])"
              db.exec("CREATE OR REPLACE TRIGGER #{TRIGGER_NAME} AFTER INSERT OR DELETE ON #{table.quoted} FOR EACH ROW EXECUTE FUNCTION #{TRIGGER_PROC}")
              db.exec("CREATE OR REPLACE TRIGGER #{UPDATE_TRIGGER_NAME} AFTER UPDATE ON #{table.quoted} FOR EACH ROW WHEN (#{condition}) EXECUTE FUNCTION #{TRIGGER_PROC}")
              fingerprint = db.scalar("SELECT tgqual::text FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2", args: [table.quoted, UPDATE_TRIGGER_NAME]).as(String)
              comment = sql_literal(POLICY_PREFIX + UpdatePolicy.new(desired, fingerprint).to_json)
              [TRIGGER_NAME, UPDATE_TRIGGER_NAME].each do |name|
                db.exec("COMMENT ON TRIGGER #{name} ON #{table.quoted} IS #{comment}")
              end
            end
          end
        end
        true
      end
    end

    private def self.check_policy(table, current, desired, requested, expected)
      if expected
        unless current == expected.uniq.sort!
          raise ArgumentError.new("CDC update policy for #{table} changed; expected #{expected}, found #{current}")
        end
      elsif requested && !current.empty? && current != desired
        raise ArgumentError.new("Conflicting CDC update policy for #{table}: #{current} versus #{desired}; use replace_cdc_update_policy")
      end
    end

    private def self.restore_metadata(db, table, metadata : UpdatePolicy)
      comment = POLICY_PREFIX + metadata.to_json
      [TRIGGER_NAME, UPDATE_TRIGGER_NAME].each do |name|
        existing = db.scalar("SELECT obj_description(oid, 'pg_trigger') FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2", args: [table.quoted, name]).as(String?)
        unless existing == comment
          db.exec("COMMENT ON TRIGGER #{name} ON #{table.quoted} IS #{sql_literal(comment)}")
        end
      end
    end

    private def self.drop_trigger(conn, table : QualifiedTable, lock_timeout, attempts, backoff_ms) : Bool
      with_ddl_retry("disable_cdc_for", table, attempts, backoff_ms) do
        conn.transaction do |tx|
          db = tx.connection
          db.exec("SELECT set_config('lock_timeout', $1, true)", args: [lock_timeout])
          db.exec(ADVISORY_LOCK_SQL, args: [TRIGGER_LOCK_SPACE, table.quoted])
          db.exec(sprintf(DROP_TRIGGER, table.quoted))
          db.exec("DROP TRIGGER IF EXISTS #{UPDATE_TRIGGER_NAME} ON #{table.quoted}")
        end
        true
      end
    end

    private struct UpdatePolicy
      include JSON::Serializable
      getter columns : Array(String)
      getter condition : String

      def initialize(@columns, @condition)
      end
    end

    # Redundant trigger comments let either trigger be repaired after accidental removal.
    # Metadata is deployment state; ordinary subscribers never supply a separate list.
    private def self.installed_policy(db, table) : UpdatePolicy?
      policies = [] of UpdatePolicy
      has_update = false
      db.query_each("SELECT tgname, obj_description(oid, 'pg_trigger') FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname IN ($2, $3)", args: [table.quoted, TRIGGER_NAME, UPDATE_TRIGGER_NAME]) do |row|
        name = row.read(String)
        has_update ||= name == UPDATE_TRIGGER_NAME
        if comment = row.read(String?)
          if comment.starts_with?(POLICY_PREFIX)
            policies << UpdatePolicy.from_json(comment[POLICY_PREFIX.size..])
          elsif comment.starts_with?("eventbus:update-policy:")
            raise ArgumentError.new("Unsupported CDC update policy metadata for #{table}")
          end
        end
      end
      if policies.empty?
        raise ArgumentError.new("CDC update policy metadata missing for #{table}; explicitly restore trigger metadata before registration") if has_update
        return
      end
      policy = policies.first
      unless policies.all? { |entry| entry.columns == policy.columns && entry.condition == policy.condition }
        raise ArgumentError.new("Inconsistent CDC update policy metadata for #{table}")
      end
      policy
    end

    private def self.validate_columns(db, table, columns)
      return if columns.empty?
      actual = [] of String
      db.query_each("SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped", args: [table.quoted]) do |row|
        actual << row.read(String)
      end
      columns.each do |column|
        unless actual.includes?(column) && column != "id"
          raise ArgumentError.new("Invalid ignored update column #{column.inspect} for #{table}; must exist and cannot be id")
        end
      end
    end

    private def self.sql_literal(value : String) : String
      "E'" + value.gsub("\\", "\\\\").gsub("'", "''") + "'"
    end

    private def self.trigger_exists?(db, table, name) : Bool
      db.scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2)", args: [table.quoted, name]).as(Bool)
    end

    private def self.trigger_matches?(db, table, name, type, condition : String?) : Bool
      return false if name == UPDATE_TRIGGER_NAME && condition.nil?
      db.scalar(<<-SQL, args: [table.quoted, name, type, condition]).as(Bool)
        SELECT EXISTS(SELECT 1 FROM pg_trigger
        WHERE tgrelid = $1::regclass AND tgname = $2 AND tgtype = $3
          AND NOT tgisinternal AND tgenabled = 'O'
          AND tgfoid = to_regprocedure('#{TRIGGER_PROC}')
          AND tgnargs = 0 AND tgattr = ''::int2vector
          AND tgqual::text IS NOT DISTINCT FROM $4::text)
        SQL
    end

    # All DDL runs on a single dedicated (non-pooled) connection so transaction-scoped
    # settings and advisory locks are guaranteed to apply to the statements that follow.
    private def self.with_ddl_connection(url, & : DB::Connection -> Bool) : Bool
      conn = DB.connect(url)
      begin
        yield conn
      ensure
        conn.close
      end
    rescue ex : DB::ConnectionRefused
      Log.error { "Unable to connect Database url #{url}" }
      Log.error { ex.inspect_with_backtrace }
      false
    end

    private def self.with_ddl_retry(operation, table : QualifiedTable, attempts : Int32, backoff_ms : Int32, & : -> Bool) : Bool
      attempt = 0
      loop do
        attempt += 1
        begin
          return yield
        rescue ex
          case sqlstate(ex)
          when SQLSTATE_LOCK_NOT_AVAILABLE
            if attempt >= attempts
              Log.error(exception: ex) { "#{operation} #{table.quoted}: lock timeout, giving up after #{attempt} attempts" }
              return false
            end
            delay = backoff_delay(attempt, backoff_ms)
            Log.warn { "#{operation} #{table.quoted}: lock timeout on attempt ##{attempt}, retrying in #{delay.total_milliseconds.to_i}ms" }
            sleep(delay)
          when SQLSTATE_UNDEFINED_TABLE
            Log.error { "#{operation} #{table.quoted}: table does not exist" }
            return false
          else
            raise ex
          end
        end
      end
    end

    # Jittered exponential backoff prevents concurrently booting services retrying in
    # lockstep; the shift is clamped so configured attempt counts cannot overflow.
    private def self.backoff_delay(attempt : Int32, backoff_ms : Int32) : Time::Span
      delay_ms = backoff_ms.to_f * (1_i64 << Math.min(attempt - 1, 10)) * rand(0.5..1.5)
      Math.min(delay_ms, 30_000.0).milliseconds
    end

    private def self.sqlstate(ex : Exception?) : String?
      while ex
        return ex.field_message(:code) if ex.is_a?(::PQ::PQError)
        ex = ex.cause
      end
    end

    private def self.setup_eventbus(conn, url) : Nil
      return if @@setup_done.includes?(url)
      @@setup_mutex.synchronize do
        return if @@setup_done.includes?(url)
        conn.transaction do |tx|
          db = tx.connection
          db.exec("SELECT set_config('lock_timeout', $1, true)", args: [LOCK_TIMEOUT])
          db.exec("SELECT pg_advisory_xact_lock($1)", args: [SETUP_LOCK_KEY])
          SETUP_STATEMENTS.each { |sql| db.exec(sql) }
        end
        @@setup_done << url
      end
    end

    private def self.list_cdc_tables(conn) : Array(QualifiedTable)
      tables = [] of QualifiedTable
      conn.query_each(LIST_TABLES_SQL) do |row|
        tables << QualifiedTable.new(row.read(String), row.read(String))
      end
      tables
    end

    # A table identity shared by the catalog pre-check and the DDL so they can never
    # disagree. Accepts bare ("users") or qualified ("tenant.users") names; bare names
    # resolve to the public schema, matching what pg-orm passes.
    private record QualifiedTable, schema : String, name : String do
      def self.parse(table : String) : QualifiedTable
        match = /\A("(?:[^"]|"")*"|[^".]+)(?:\.("(?:[^"]|"")*"|[^".]+))?\z/.match(table)
        raise ArgumentError.new("Invalid table identifier #{table.inspect}") unless match
        first = unquote(match[1])
        second = match[2]?.try { |part| unquote(part) }
        parts = second ? [first, second] : ["public", first]
        unless parts.all? { |part| !part.empty? && part.bytesize <= 63 && !part.includes?('\0') }
          raise ArgumentError.new("Invalid table identifier #{table.inspect}")
        end
        new(parts[0], parts[1])
      end

      protected def self.unquote(part : String) : String
        if part.size >= 2 && part.starts_with?('"') && part.ends_with?('"')
          part[1..-2].gsub(%(""), %("))
        else
          part
        end
      end

      def quoted : String
        %("#{schema.gsub(%("), %(""))}"."#{name.gsub(%("), %(""))}")
      end

      def to_s(io : IO) : Nil
        io << quoted
      end
    end

    # The lock key is derived from the relation oid (not the name string) so it can
    # never disagree with the catalog identity used by the pre-check and the DDL.
    ADVISORY_LOCK_SQL = "SELECT pg_advisory_xact_lock($1, hashtext(($2::regclass::oid)::text))"

    DROP_TRIGGER = "DROP TRIGGER IF EXISTS #{TRIGGER_NAME} ON %s;"

    # CREATE OR REPLACE TRIGGER (PostgreSQL >= 14) swaps the trigger atomically: unlike
    # DROP + CREATE there is no trigger-less window losing events, and it never takes
    # ACCESS EXCLUSIVE so readers are never blocked.
    CREATE_TRIGGER = <<-SQL

      CREATE OR REPLACE TRIGGER #{TRIGGER_NAME} AFTER INSERT OR UPDATE OR DELETE ON %s
      FOR EACH ROW EXECUTE FUNCTION #{TRIGGER_PROC};

      SQL

    LIST_TABLES_SQL = <<-SQL
      SELECT DISTINCT t.table_schema, t.table_name
      FROM information_schema.tables t
      INNER JOIN information_schema.columns c
        ON c.table_schema = t.table_schema
        AND c.table_name = t.table_name
      WHERE t.table_type = 'BASE TABLE'
        AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
        AND t.table_schema NOT LIKE 'pg_toast%'
        AND t.table_name != 'eventbus_cdc_events'
        AND c.column_name = 'id'
      ORDER BY t.table_schema, t.table_name
      SQL

    SETUP_STATEMENTS = [
      <<-SQL,
        CREATE TABLE IF NOT EXISTS eventbus_cdc_events(
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          event_schema VARCHAR NOT NULL,
          event_table VARCHAR NOT NULL,
          event_action VARCHAR NOT NULL,
          row_id TEXT NOT NULL,
          event_data JSONB NOT NULL,
          created_at TIMESTAMP NOT NULL
        );
        SQL
      <<-SQL,
        ALTER TABLE eventbus_cdc_events ADD COLUMN IF NOT EXISTS change_data JSONB;
        SQL
      <<-SQL,
        CREATE INDEX IF NOT EXISTS eventbus_cdc_events_created_at_idx
        ON eventbus_cdc_events (created_at);
        SQL
      <<-SQL,
        CREATE OR REPLACE FUNCTION public.eventbus_cdc_run_cleanup() RETURNS void AS $$
        BEGIN
            DELETE FROM eventbus_cdc_events where created_at < CURRENT_TIMESTAMP - INTERVAL '#{RETENTION}';
        END;
        $$ LANGUAGE plpgsql;
        SQL
      <<-SQL,
        CREATE OR REPLACE FUNCTION public.eventbus_cdc_cleanup() RETURNS TRIGGER AS $$
        BEGIN
            -- probabilistic gate: retention is amortised across inserts instead of
            -- running a DELETE inside every writer's transaction
            IF random() < #{CLEANUP_PROBABILITY} THEN
              PERFORM public.eventbus_cdc_run_cleanup();
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
        SQL
      <<-SQL,
        DO $$
        BEGIN
          IF NOT EXISTS(SELECT * FROM information_schema.triggers
            WHERE event_object_table = 'eventbus_cdc_events'
            AND trigger_name = 'eventbus_cdc_events_trigger'
          )
          THEN
            CREATE TRIGGER eventbus_cdc_events_trigger AFTER INSERT ON eventbus_cdc_events
            EXECUTE PROCEDURE public.eventbus_cdc_cleanup();
          END IF;
        END;
        $$ LANGUAGE plpgsql;
        SQL
      <<-SQL,
        CREATE OR REPLACE FUNCTION public.eventbus_notify_change() RETURNS TRIGGER AS $$
        DECLARE
            data record;
            log_id integer;
            notification json;
            change json;
        BEGIN
            -- Convert the old or new row to JSON, based on the kind of action.
            -- Action = DELETE?             -> OLD row
            -- Action = INSERT or UPDATE?   -> NEW row
            IF (TG_OP = 'DELETE') THEN
                data =  OLD;
            ELSE
                data =  NEW;
            END IF;

            IF (TG_OP = 'UPDATE') THEN
              change := (SELECT JSON_AGG(src) FROM (SELECT pre.key AS field, pre.value AS old, post.value AS new
                                  FROM jsonb_each(to_jsonb(OLD)) AS pre
                                  CROSS JOIN jsonb_each(to_jsonb(NEW)) AS post
                                  WHERE pre.key = post.key AND pre.value IS DISTINCT FROM post.value) src);
            ELSE
              change := NULL;
            END IF;

           -- Save data to events table
           INSERT INTO eventbus_cdc_events(event_schema, event_table, event_action, row_id, created_at, event_data, change_data)
                VALUES (TG_TABLE_SCHEMA,TG_TABLE_NAME, LOWER(TG_OP), data.id, CURRENT_TIMESTAMP, to_jsonb(data), change)
                RETURNING id INTO log_id;
           -- Construct json payload
           -- note that here can be done projection
            notification = json_build_object(
                                'logid', log_id,
                                'timestamp',CURRENT_TIMESTAMP,
                                'schema',TG_TABLE_SCHEMA,
                                'table',TG_TABLE_NAME,
                                'action', LOWER(TG_OP),
                                'id', data.id);

             -- note that channel name MUST be lowercase, otherwise pg_notify() won't work
            -- Execute pg_notify(channel, notification)
            PERFORM pg_notify('cdc_events',notification::text);
            -- Result is ignored since we are invoking this in an AFTER trigger
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
        SQL
      <<-SQL,
        DROP FUNCTION IF EXISTS public.eventbus_cdc_for_all_tables();
        SQL
    ]
  end
end
