class EventMachineThriftClient < AbstractThriftClient
  # This class is works with the thrift core eventmachinetransport
  # not the other eventmachine class in thrift_client

  def connect!
    @current_server = next_live_server
    host, port = @current_server.connection_string.split(':')
    raise ArgumentError, 'Servers must be in the form "host:port"' unless host and port

    @connection = Thrift::EventMachineTransport.connect(@client_class, host, port)
    @connection.set_pending_connect_timeout(@options[:connect_timeout])
    @connection.callback(&@callbacks[:post_connect]) if @callbacks[:post_connect]
  end

  def _handled_proxy(method_name, d, tries, *args)
    disconnect_on_max! if @options[:server_max_requests] && @request_count >= @options[:server_max_requests]
    exception_handler = proc do |err|
      if err == :timeout || @options[:exception_classes].include?(err.class)
        disconnect_on_error!

        tries -= 1
        if tries > 0
          _handled_proxy(method_name, d, tries, *args)
        else
          raise_or_errback_or_default(err, method_name, d)
        end
      else
        raise_or_errback_or_default(err, method_name, d)
      end
    end

    if !@connection || @connection.error?
      connect!
      @connection.errback(&exception_handler)
    end
    @connection.callback do |client|
      @request_count += 1
      deferred = client.send(method_name, *args)
      deferred.errback(&exception_handler)
      deferred.callback {|*args| d.succeed(*args) }

      timeout = @options[:timeout_overrides][method_name.to_sym] || @options[:timeout]
      deferred.timeout(timeout, :timeout)
    end
  end

  def raise_or_errback_or_default(e, method_name, d)
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
