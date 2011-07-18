require 'net/http'
require File.dirname(__FILE__) + '/operation/meta_operation'
require File.dirname(__FILE__) + '/operation/table_operation'
require File.dirname(__FILE__) + '/operation/row_operation'
require File.dirname(__FILE__) + '/operation/scanner_operation'

module Stargate
  class Client
    include Operation::MetaOperation
    include Operation::TableOperation
    include Operation::RowOperation
    include Operation::ScannerOperation

    attr_reader :url, :connection

    def initialize(url = "http://localhost:8080", opts = {})
      @url = URI.parse(url)
      @default_headers = {}

      unless @url.kind_of? URI::HTTP
        raise "invalid http url: #{url}"
      end

      # Not actually opening the connection yet, just setting up the persistent connection.
      if opts[:proxy]
        proxy_address, proxy_port = opts[:proxy].split(':')
        @connection = Net::HTTP.Proxy(proxy_address, proxy_port).new(@url.host, @url.port)
      else
        @connection = Net::HTTP.new(@url.host, @url.port)
      end
      @connection.read_timeout = opts[:timeout] if opts[:timeout]

      @default_headers['Accept-Encoding'] = 'identity' if opts.has_key?(:http_compression) && !opts[:http_compression]
    end

    def get(path, options = {})
      headers = {"Accept" => "application/json"}.merge(@default_headers).merge(options)
      safe_request { @connection.get(@url.path + path, headers) }
    end

    def get_response(path, options = {})
      headers = {"Accept" => "application/json"}.merge(@default_headers).merge(options)
      safe_response { @connection.get(@url.path + path, headers) }
    end

    def post(path, data = nil, options = {})
      headers = {'Content-Type' => 'text/xml'}.merge(@default_headers).merge(options)
      safe_request { @connection.post(@url.path + path, data, headers) }
    end

    def post_response(path, data = nil, options = {})
      headers = {'Content-Type' => 'text/xml'}.merge(@default_headers).merge(options)
      safe_response { @connection.post(@url.path + path, data, headers) }
    end

    def delete(path, options = {})
      headers = @default_headers.merge(options)
      safe_request { @connection.delete(@url.path + path, headers) }
    end

    def delete_response(path, options = {})
      headers = @default_headers.merge(options)
      safe_response { @connection.delete(@url.path + path, headers) }
    end

    def put(path, data = nil, options = {})
      headers = {'Content-Type' => 'text/xml'}.merge(@default_headers).merge(options)
      safe_request { @connection.put(@url.path + path, data, headers) }
    end

    def put_response(path, data = nil, options = {})
      headers = {'Content-Type' => 'text/xml'}.merge(@default_headers).merge(options)
      safe_response { @connection.put(@url.path + path, data, headers) }
    end

    private

      def safe_response(&block)
        begin
          yield
        rescue Errno::ECONNREFUSED
          raise ConnectionNotEstablishedError, "can't connect to #{@url}"
        rescue Timeout::Error => e
          puts e.backtrace.join("\n")
          raise ConnectionTimeoutError, "execution expired. Maybe query disabled tables"
        end
      end

      def safe_request(&block)
        response = safe_response{ yield block }

        case response
        when Net::HTTPSuccess
          response.body
        else
          response.error!
        end
      end

  end
end
