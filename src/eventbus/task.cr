class EventBus
  class Task
    getter receivers : Array(EventHandler)
    getter event : DBEvent
    getter pool : DB::Database

    def initialize(@event, @receivers, @pool, @retry_count : Int32)
    end

    def id
      event.logid.to_s
    end

    def run
      if evt = enrich(event)
        receivers.each do |h|
          h.on_event(evt)
        end
      else
        Log.error { "Unable to dispatch event to listeners due to problem reading the event details for id #{event.logid}" }
      end
    end

    private def enrich(evt : DBEvent)
      data = fetch(evt)
      if data
        Event.new(evt.timestamp, evt.schema, evt.table, evt.action, evt.id, *data)
      end
    end

    private def fetch(evt : DBEvent) : Tuple(String, String?)?
      attempt = 0
      while (@retry_count <= 0 || attempt <= @retry_count)
        begin
          attempt += 1
          res = connection(&.query_one "select event_data, change_data from public.eventbus_cdc_events where id = $1", evt.logid, as: {JSON::Any, JSON::Any?})
          return {res[0].to_json, res[1].try &.to_json}
        rescue err
          Log.warn { "Fetching record id: #{evt.logid} from DB. Received error '#{err.message || err.class.name}'. retrying attempt ##{attempt} after 1 second" }
          sleep(1.second)
        end
      end
      Log.error { "Giving up after attempting #{attempt} retries to re-connect to database and fetch record id: #{evt.logid}." }
      nil
    end

    private def connection
      pool.using_connection { |_db| yield _db }
    end
  end

  class TaskRunner
    WAIT_TIME = 5.second
    getter lock : Mutex
    getter tasks : Deque(Task)
    getter running : Array(String)

    def initialize(@job_count : Int32)
      @lock = Mutex.new
      @tasks = Deque(Task).new
      @job_queue = Channel(Task).new(@job_count)
      @running = Array(String).new
      @terminate_queue = Channel(Nil).new(@job_count)
      @stop_chan = Channel(Nil).new
    end

    def add_task(task : Task)
      lock.synchronize { tasks.push(task) }
    end

    def has?(task_id : String, pending_only = false) : Bool
      lock.synchronize {
        task = tasks.any? { |t| t.id == task_id }
        return task if pending_only
        task || running.includes?(task_id)
      }
    end

    def cancel_task(task_id : String) : Nil
      lock.synchronize { tasks.reject! { |t| t.id == task_id } }
    end

    def start
      @job_count.times do |i|
        spawn(name: "Worker-#{i + 1}") { handle_job(@job_queue, @terminate_queue, WAIT_TIME) }
      end
      spawn(name: "JobRunner") { run(@stop_chan) }
    end

    def stop
      spawn { @stop_chan.send(nil) }
    end

    def get_job?
      lock.synchronize do
        work = tasks.pop?
        if w = work
          running << w.id
        end
        work
      end
    end

    def job_done(id : String) : Nil
      lock.synchronize { running.delete(id) }
    end

    private def run(terminate : Channel(Nil))
      loop do
        if task = get_job?
          begin
            select
            when terminate.receive?
              Log.info { "JobRunner received shutdown request" }
              break
            when @job_queue.send(task)
              Log.info { {message: "Task scheduled to run by worker.", task: task.id} }
              next
            end
          rescue Channel::ClosedError
            Log.error { "ERROR: Job runner queue channel closed" }
            break
          end
        end
        sleep 0.1
      end
      Log.info { "Terminating job workers" }
      @job_count.times { @terminate_queue.send(nil) }
    end

    private def handle_job(chan : Channel(Task), terminate : Channel(Nil), wait_time : Time::Span)
      loop do
        select
        when task = chan.receive
          task.run
          job_done(task.id)
        when terminate.receive?
          Log.info { "shutting down job worker" }
          break
        when timeout wait_time
          sleep 0.1
        end
      rescue Channel::ClosedError
        Log.error { "shutting down job worker #{Fiber.current.name} due to channel closed" }
        break
      end
    end
  end
end
