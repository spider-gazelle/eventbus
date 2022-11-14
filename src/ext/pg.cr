module PG
  class ListenConnection
    @conn : PG::Connection

    def self.new(url, *channels : String, blocking : Bool = false, &blk : PQ::Notification ->)
      new(url, channels, blocking, &blk)
    end

    def initialize(url, @channels : Enumerable(String), @blocking : Bool = false, &blk : PQ::Notification ->)
      @conn = DB.connect(url).as(PG::Connection)
      @conn.on_notification(&blk)
      start unless @blocking
    end

    # Close the connection.
    def close
      @conn.close
    rescue
    end

    def start
      @conn.listen(@channels, blocking: @blocking)
    end
  end
end
