module EventBus
  module EventBusSchema
    Log = App::Log.for("EventInitalizer")

    def self.init : Bool
      begin
        conn = DB.open(App::PG_DATABASE_URL)
        found = alread_initialized?(conn)
        Log.info { "DB Schema initialized? - #{found ? "Yes" : "No"}" }
        return true if found
        Log.info { "Initializing DB Schema" }
        conn.exec(SCHEMA_CDC)
        conn.exec(SCHEMA_TRIGGER)
        conn.exec("select public.create_cdc_for_all_tables()")
        Log.info { "DB Schema initialization completed" }
      rescue ex : DB::ConnectionRefused
        Log.error { "Unable to connect Database url #{App::PG_DATABASE_URL}" }
        Log.error { ex.inspect_with_backtrace }
        return false
      ensure
        conn.try &.close
      end
      true
    end

    private def self.alread_initialized?(conn) : Bool
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
    PERFORM pg_notify('cdc_events',notification::text);
    -- Result is ignored since we are invoking this in an AFTER trigger
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

SQL

    SCHEMA_TRIGGER = <<-SQL
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
  end
end
