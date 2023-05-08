require "db"
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
        setup_eventbus(conn)
        conn.exec("select public.eventbus_cdc_for_all_tables()")
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
        setup_eventbus(conn) unless already_initialized?(conn)
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

    def self.disable_cdc_for(url, table) : Nil
      conn = DB.open(url)
      conn.exec(sprintf(DROP_TRIGGER, table)) rescue nil
    ensure
      conn.try &.close
    end

    private def self.setup_eventbus(conn)
      EVENT_LOGGER_SETUP.each { |sql| conn.exec(sql) }
    end

    private def self.already_initialized?(conn) : Bool
      conn.exec(%(
        select 'public.eventbus_cdc_events'::regclass, 'public.eventbus_cdc_cleanup'::regproc,
        'eventbus_cdc_for_all_tables'::regproc, 'eventbus_notify_change'::regproc;
        ))
      true
    rescue
      false
    end

    DROP_TRIGGER   = "DROP TRIGGER IF EXISTS eventbus_notify_change_event ON \"%s\";"
    CREATE_TRIGGER = <<-SQL

CREATE TRIGGER eventbus_notify_change_event AFTER INSERT OR UPDATE OR DELETE ON "%s"
FOR EACH ROW EXECUTE PROCEDURE public.eventbus_notify_change();

SQL

    EVENT_LOGGER_SETUP = [
      %(
        CREATE TABLE IF NOT EXISTS eventbus_cdc_events(
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          event_schema VARCHAR NOT NULL,
          event_table VARCHAR NOT NULL,
          event_action VARCHAR NOT NULL,
          row_id TEXT NOT NULL,
          event_data JSONB NOT NULL,
          created_at TIMESTAMP NOT NULL
        );
      ),
      %(
        ALTER TABLE eventbus_cdc_events ADD COLUMN IF NOT EXISTS change_data JSONB;
      ),
      %(
        CREATE OR REPLACE FUNCTION public.eventbus_cdc_cleanup() RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM eventbus_cdc_events where created_at < CURRENT_TIMESTAMP - INTERVAL '1 day';
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
      ),
      %(
        DROP TRIGGER IF EXISTS eventbus_cdc_events_trigger ON eventbus_cdc_events;
      ),
      %(
        CREATE TRIGGER eventbus_cdc_events_trigger AFTER INSERT ON eventbus_cdc_events
          EXECUTE PROCEDURE public.eventbus_cdc_cleanup();
      ), %(
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
      ),
      %(
          -- Instead of manually creating triggers for each table, create CDC Trigger For All tables with id column

          CREATE OR REPLACE FUNCTION public.eventbus_cdc_for_all_tables() RETURNS void AS $$
          DECLARE
            trigger_statement TEXT;
          BEGIN
            FOR trigger_statement IN SELECT
              'DROP TRIGGER IF EXISTS eventbus_notify_change_event ON '
              || tab_name || ';'
              || 'CREATE TRIGGER eventbus_notify_change_event AFTER INSERT OR UPDATE OR DELETE ON '
              || tab_name
              || ' FOR EACH ROW EXECUTE PROCEDURE public.eventbus_notify_change();' AS trigger_creation_query
            FROM (
              SELECT
                quote_ident(t.table_schema) || '.' || quote_ident(t.table_name) as tab_name
              FROM
                information_schema.tables t, information_schema.columns c
              WHERE
                t.table_schema NOT IN ('pg_catalog', 'information_schema')
                AND t.table_schema NOT LIKE 'pg_toast%'
                AND t.table_name != 'eventbus_cdc_events'
                AND c.table_name = t.table_name AND c.column_name='id'
            ) as TableNames
            LOOP
              EXECUTE  trigger_statement;
            END LOOP;
          END;
          $$ LANGUAGE plpgsql;
      ),
    ]
  end
end
