require "pg"
require "log"

class EventBus
  private module PG
    Log     = ::Log.for("PGInitalizer")
    CHANNEL = "cdc_events"

    def self.ensure_cdc_for_all_tables(url) : Bool
      begin
        conn = DB.open(url)
        found = already_initialized?(conn)
        Log.info { "DB Schema initialized? - #{found ? "Yes" : "No"}" }
        return true if found
        Log.info { "Initializing DB Schema" }
        conn.exec(SCHEMA_CDC)
        conn.exec(SCHEMA_TRIGGER_4_ALL)
        conn.exec("select public.create_cdc_for_all_tables()")
        Log.info { "DB Schema initialization completed" }
      rescue ex : DB::ConnectionRefused
        Log.error { "Unable to connect Database url #{url}" }
        Log.error { ex.inspect_with_backtrace }
        return false
      ensure
        conn.try &.close
      end
      true
    end

    def self.ensure_cdc_for(url, table) : Bool
      begin
        conn = DB.open(url)
        conn.exec(SCHEMA_CDC) unless already_initialized?(conn)
        conn.exec(sprintf(DROP_TRIGGER, table))
        conn.exec(sprintf(CREATE_TRIGGER, table))
      rescue ex : DB::ConnectionRefused
        Log.error { "Unable to connect Database url #{url}" }
        Log.error { ex.inspect_with_backtrace }
        return false
      ensure
        conn.try &.close
      end
      true
    end

    private def self.already_initialized?(conn) : Bool
      conn.exec("select 'create_cdc_for_all_tables'::regproc;")
      true
    rescue
      false
    end

    SCHEMA_CDC = <<-SQL

CREATE OR REPLACE FUNCTION public.notify_change() RETURNS TRIGGER AS $$

DECLARE
    data record;
    notification json;
BEGIN
    -- Convert the old or new row to JSON, based on the kind of action.
    -- Action = DELETE?             -> OLD row
    -- Action = INSERT or UPDATE?   -> NEW row
    IF (TG_OP = 'DELETE') THEN
        data =  OLD;
    ELSE
        data =  NEW;
    END IF;

   -- Construct json payload
   -- note that here can be done projection
    notification = json_build_object(
                        'timestamp',CURRENT_TIMESTAMP,
                        'schema',TG_TABLE_SCHEMA,
                        'table',TG_TABLE_NAME,
                        'action', LOWER(TG_OP),
                        'id', data.id);

     -- note that channel name MUST be lowercase, otherwise pg_notify() won't work
    -- Execute pg_notify(channel, notification)
    PERFORM pg_notify('#{CHANNEL}',notification::text);
    -- Result is ignored since we are invoking this in an AFTER trigger
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

SQL

    SCHEMA_TRIGGER_4_ALL = <<-SQL
-- Instead of manually creating triggers for each table, create CDC Trigger For All tables with id column

CREATE OR REPLACE FUNCTION public.create_cdc_for_all_tables() RETURNS void AS $$

DECLARE
  trigger_statement TEXT;
BEGIN
  FOR trigger_statement IN SELECT
    'DROP TRIGGER IF EXISTS notify_change_event ON '
    || tab_name || ';'
    || 'CREATE TRIGGER notify_change_event AFTER INSERT OR UPDATE OR DELETE ON '
    || tab_name
    || ' FOR EACH ROW EXECUTE PROCEDURE public.notify_change();' AS trigger_creation_query
  FROM (
    SELECT
      quote_ident(t.table_schema) || '.' || quote_ident(t.table_name) as tab_name
    FROM
      information_schema.tables t, information_schema.columns c
    WHERE
      t.table_schema NOT IN ('pg_catalog', 'information_schema')
      AND t.table_schema NOT LIKE 'pg_toast%'
      AND c.table_name = t.table_name AND c.column_name='id'
  ) as TableNames
  LOOP
    EXECUTE  trigger_statement;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

SQL

    DROP_TRIGGER   = "DROP TRIGGER IF EXISTS notify_change_event ON %s;"
    CREATE_TRIGGER = <<-SQL

CREATE TRIGGER notify_change_event AFTER INSERT OR UPDATE OR DELETE ON %s
FOR EACH ROW EXECUTE PROCEDURE public.notify_change();

SQL
  end
end
