require 'net/http'
require 'uri'
require 'yajl'
require 'fluent/plugin/output'

class Fluent::Plugin::HTTPOutput < Fluent::Plugin::Output
  Fluent::Plugin.register_output('elk', self)

  helpers :compat_parameters

  DEFAULT_BUFFER_TYPE = "memory"

  def initialize
    super
  end

  config_param :host, :string

  config_param :port, :string
  
  config_param :index_name, :string

  # Set Net::HTTP.verify_mode to `OpenSSL::SSL::VERIFY_NONE`
  config_param :ssl_no_verify, :bool, :default => false

  # Simple rate limiting: ignore any records within `rate_limit_msec`
  # since the last one.
  config_param :rate_limit_msec, :integer, :default => 0

  # Raise errors that were rescued during HTTP requests?
  config_param :raise_on_error, :bool, :default => true

  # ca file to use for https request
  config_param :cacert_file, :string, :default => ''

  config_param :token, :string, :default => ''
  # Switch non-buffered/buffered plugin
  config_param :buffered, :bool, :default => false

  config_section :buffer do
    config_set_default :@type, DEFAULT_BUFFER_TYPE
    config_set_default :chunk_keys, ['tag']
  end

  def configure(conf)
    compat_parameters_convert(conf, :buffer)
    super

    @ssl_verify_mode = if @ssl_no_verify
                         OpenSSL::SSL::VERIFY_NONE
                       else
                         OpenSSL::SSL::VERIFY_PEER
                       end

    @ca_file = @cacert_file
    @last_request_time = nil
    raise Fluent::ConfigError, "'tag' in chunk_keys is required." if !@chunk_key_tag && @buffered
  end

  def start
    super
  end

  def shutdown
    super
  end


  def set_body(req, tag, time, record)
    req.body = Yajl.dump(data)
    req['Content-Type'] = 'application/json'
    req
  end


  def create_request(tag, time, record)
    hash = {
      a: 'logs',
      b: @index_name
    }
    uri = URI::HTTP.build(host: @host, port: @port, query: hash.to_query)
    req = Net::HTTP::Post.new(uri.request_uri)
    set_body(req, tag, time, record)
    return req, uri
  end

  def send_request(req, uri)
    is_rate_limited = (@rate_limit_msec != 0 and not @last_request_time.nil?)
    if is_rate_limited and ((Time.now.to_f - @last_request_time) * 1000.0 < @rate_limit_msec)
      log.info('Dropped request due to rate limiting')
      return
    end

    res = nil

    begin
      req['authorization'] = "ELK #{@token}"
      @last_request_time = Time.now.to_f
      res = Net::HTTP.start(uri.host, uri.port, **http_opts(uri)) {|http| http.request(req) }
      end

    rescue => e # rescue all StandardErrors
      # server didn't respond
      log.warn "raises exception: #{e.class}, '#{e.message}'"
      raise e if @raise_on_error
    else
       unless res and res.is_a?(Net::HTTPSuccess)
          res_summary = if res
                           "#{res.code} #{res.message} #{res.body}"
                        else
                           "res=nil"
                        end
          log.warn "failed to #{req.method} #{uri} (#{res_summary})"
       end #end unless
    end # end begin
  end # end send_request

  def handle_record(tag, time, record)
    req, uri = create_request(tag, time, record)
    send_request(req, uri)
  end

  def prefer_buffered_processing
    @buffered
  end

  def format(tag, time, record)
    [time, record].to_msgpack
  end

  def formatted_to_msgpack_binary?
    true
  end

  def multi_workers_ready?
    true
  end

  def process(tag, es)
    es.each do |time, record|
      handle_record(tag, time, record)
    end
  end

  def write(chunk)
    tag = chunk.metadata.tag
    chunk.msgpack_each do |time, record|
      handle_record(tag, time, record)
    end
  end
end
