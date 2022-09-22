require "spec"
require "../src/config"

include EventBus

Spec.before_suite { run_sql("CREATE TABLE IF NOT EXISTS spec_test(id int, name VARCHAR(100))") }
Spec.after_suite { run_sql("drop table if exists spec_test") }
Spec.before_each { run_sql("delete from spec_test") }

def run_sql(cmd)
  DB.open(App::PG_DATABASE_URL) do |db|
    db.exec(cmd)
  end
end

def insert_rec(id : Int)
  run_sql("insert into spec_test values(#{id}, 'Testing')")
end
