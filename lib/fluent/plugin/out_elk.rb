require 'net/http'
require 'uri'
require 'yajl'
require 'date'
require 'fluent/plugin/output'
begin
  require_relative 'oj_serializer'
rescue LoadError
end

module Fluent::Plugin
 class ELKOutput < Output

    Fluent::Plugin.register_output('elk', self)

    DEFAULT_BUFFER_TYPE = "memory"
    BODY_DELIMITER = "\n".freeze

    helpers :compat_parameters, :record_accessor

    config_param :host, :string

    config_param :port, :string
    config_param :use_ssl, :bool, :default => true
    config_param :index_pattern, :string

    # Set Net::HTTP.verify_mode to `OpenSSL::SSL::VERIFY_NONE`
    config_param :ssl_no_verify, :bool, :default => false

    # Simple rate limiting: ignore any records within `rate_limit_msec`
    # since the last one.
    config_param :rate_limit_msec, :integer, :default => 0

    # Raise errors that were rescued during HTTP requests?
    config_param :raise_on_error, :bool, :default => true

    config_param :token, :string, :default => ''
    
    # Switch non-buffered/buffered plugin
    config_param :buffered, :bool, :default => false

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
      config_set_default :chunk_keys, ['tag']
      config_set_default :timekey_use_utc, true
    end
    
    def initialize
      super
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer)
      super

      begin
        require 'oj'
        @dump_proc = Oj.method(:dump)
      rescue LoadError
        @dump_proc = Yajl.method(:dump)
      end
      
    end

    def start
      super
    end

    def shutdown
      super
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

    def extract_index_postfix(record)
      if record['kubernetes'].has_key?('namespace_name') 
        return record['kubernetes']['namespace_name']
       end
      return 'default'
    end

    def record_to_json(tag, time, record)
      dt = Time.at(time).to_datetime
      record['@timestamp'] = dt.iso8601(9)
      record['tag'] = tag
      data = @dump_proc.call(record)
      data
    end
    def get_index_name(index_postfix)
      return @index_pattern +'-'+ index_postfix + Time.now.strftime('-%Y.%m.%d')
    end
    def create_request(index_name)
      uri = URI::HTTP.build({:host => @host, :port => @port, :path => '/logs/' + index_name, :use_ssl => @use_ssl})
      req = Net::HTTP::Post.new(uri.to_s)
      return req, uri
    end

    def send_request(req, uri)
      is_rate_limited = (@rate_limit_msec != 0 and not @last_request_time.nil?)
      if is_rate_limited and ((Time.now.to_f - @last_request_time) * 1000.0 < @rate_limit_msec)
        log.info('Dropped request due to rate limiting')
        return
      end

      res = nil
      log.info('body: '+ req.body)
      begin
        req['authorization'] = "ELK #{@token}"
        req['Content-Type'] = 'application/json'
        @last_request_time = Time.now.to_f
        res = Net::HTTP.start(uri.host, uri.port, :read_timeout => 500, :use_ssl => @use_ssl) {|http| http.request(req) }

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
        end
      end
    end 

    def write(chunk)
      tag = chunk.metadata.tag
      data_list = []
      chunk.msgpack_each {|time, record|
        index_postfix = extract_index_postfix(record)
        index_name = get_index_name(index_postfix)
        msg = record_to_json(tag, time, record)
        data_list << [index_name, msg]
      }
      data_list.group_by(&:first)
      .map { |c, xs| [c, xs.map(&:last)]}
      .each{|index,messages| 
        req, uri = create_request(index)
        data = '{"index": {"_index": "' + index + '", "_type": "_doc"}}' + BODY_DELIMITER
        messages.each{|message| data << message << BODY_DELIMITER}
        req.body = data
        log.info('uri: '+ uri.to_s)
        send_request(req, uri)
      }

    end
    
  end
end

