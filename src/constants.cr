module EventBus
  module App
    NAME = "magic-eventbus"
    {% begin %}
        VERSION = {{ `shards version "#{__DIR__}"`.chomp.stringify.downcase }}
    {% end %}

    ENVIRONMENT = ENV["SG_ENV"]? || "development"
    TEST        = ENVIRONMENT == "test"
    PRODUCTION  = ENVIRONMENT == "production"

    BUILD_TIME   = {{ system("date -u").stringify }}
    BUILD_COMMIT = {{ env("PLACE_COMMIT") || "DEV" }}

    CHANNEL = "cdc_events"

    Log         = ::Log.for(NAME)
    LOG_BACKEND = ::Log::IOBackend.new

    PG_DATABASE_URL = TEST ? ENV["PG_TEST_DATABASE_URL"] : ENV["PG_DATABASE_URL"]

    REDIS_URL = ENV["REDIS_URL"]

    class_getter? running_in_production : Bool = PRODUCTION
  end
end
