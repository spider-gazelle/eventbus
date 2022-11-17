module PG
  class ListenConnection
    def initialize(url, @channels : Enumerable(String), @blocking : Bool = false, &blk : PQ::Notification ->)
      @conn = DB.connect(url).as(PG::Connection)
      @conn.on_notification(&blk)
      start unless @blocking
    end

    def start
      @conn.listen(@channels, blocking: @blocking)
    end
  end
end

module PQ
  struct ConnInfo
    getter timeout : Int32?

    def initialize(uri : URI)
      previous_def(uri)
      if q = uri.query
        HTTP::Params.parse(q) do |key, value|
          if key == "timeout"
            @timeout = value.to_i
          end
        end
      end
    end
  end

  class Connection
    def initialize(@conninfo : ConnInfo)
      @mutex = Mutex.new
      @server_parameters = Hash(String, String).new
      @established = false
      @notice_handler = Proc(Notice, Void).new { }
      @notification_handler = Proc(Notification, Void).new { }

      begin
        if @conninfo.host[0] == '/'
          soc = UNIXSocket.new(@conninfo.host)
        else
          soc = TCPSocket.new(@conninfo.host, @conninfo.port, connect_timeout: @conninfo.timeout)
        end
        soc.sync = false
        if timeout = @conninfo.timeout
          soc.read_timeout = timeout
          soc.write_timeout = timeout
        end
      rescue e
        raise ConnectionError.new("Cannot establish connection", cause: e)
      end

      @soc = soc
      negotiate_ssl if @soc.is_a?(TCPSocket)
    end
  end
end
