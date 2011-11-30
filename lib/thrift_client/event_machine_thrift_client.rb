class EventMachineThriftClient < AbstractThriftClient
  # This class is works with the thrift core eventmachinetransport
  # not the other eventmachine class in thrift_client

  class Connection < Thrift::EventMachineTransport
    attr_reader :pending_requests
    def initialize(args={})
      @pending_requests = 0
      @shutting_down = false
      super
    end

    def incr_request
      @pending_requests += 1
    end

    def decr_request
      @pending_requests -= 1
      if @shutting_down && @pending_requests == 0
        close
      end
    end

    def close
      @shutting_down = true
      if @pending_requests == 0
        super
      end
    end
  end

  def initialize(client_class, servers, options = {})
    @error_count = 0
    @pending_shutdown_connections = []
    super
  end

  def connect!
    cleanup_pending_connections
    @current_server = next_live_server
    host, port = @current_server.connection_string.split(':')
    raise ArgumentError, 'Servers must be in the form "host:port"' unless host and port

    @connection = Connection.connect(@client_class, host, port)
    @connection.set_pending_connect_timeout(@options[:connect_timeout])
    @connection.callback(&@callbacks[:post_connect]) if @callbacks[:post_connect]
  end

  private

  def cleanup_pending_connections
    @pending_shutdown_connections.delete_if {|con| con.pending_requests == 0}
  end

  def disconnect_on_error!
    if @connection.pending_requests > 0
      @pending_shutdown_connections << @connection
    end
    @error_count += 1
    super
  end

  def _handled_proxy(method_name, d, tries, *args)
    disconnect_on_max! if @options[:server_max_requests] && @request_count >= @options[:server_max_requests]
    error_count = @error_count
    exception_handler = proc do |err|
      if err == :timeout
        err = Thrift::TransportException.new(Thrift::TransportException::TIMED_OUT, "Connection timeout")
      end

      if @options[:exception_classes].include?(err.class)
        disconnect_on_error! if @error_count == error_count

        tries -= 1
        if tries > 0
          _handled_proxy(method_name, d, tries, *args)
        else
          handle_error(err, method_name, d)
        end
      else
        handle_error(err, method_name, d)
      end
    end

    if !@connection || @connection.error?
      connect!
      @connection.errback(&exception_handler)
    end
    connection = @connection
    @connection.callback do |client|
      connection.incr_request
      deferred = client.send(method_name, *args)
      deferred.errback(&exception_handler)
      deferred.errback { connection.decr_request }
      deferred.callback do |*args|
        connection.decr_request
        d.succeed(*args)
      end

      timeout = @options[:timeout_overrides][method_name.to_sym] || @options[:timeout]
      deferred.timeout(timeout, :timeout)
    end
  end

  def handle_error(e, method_name, d)
    if @options[:raise]
      begin
        raise_wrapped_error(e)
      rescue Exception => wrapped_exception
        if @options[:raise] == :errback
          d.fail(wrapped_exception)
        else
          raise wrapped_exception
        end
      end
    else
      d.succeed(@options[:defaults][method_name.to_sym])
    end
  end

  def handled_proxy(method_name, *args)
    d = EventMachine::DefaultDeferrable.new
    tries = (@options[:retry_overrides][method_name.to_sym] || @options[:retries]) + 1
    _handled_proxy(method_name, d, tries, *args)
    return d
  end
end
