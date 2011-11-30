require File.expand_path('test_helper.rb', File.dirname(__FILE__))
require 'eventmachine'
require 'thrift/transport/event_machine_transport'
require 'thrift_client/event_machine_thrift_client'
require File.expand_path('greeter.eventmachine/greeter', File.dirname(__FILE__))


class ThriftClientTest < Test::Unit::TestCase
  def setup
    @fake_port = 1461
    @real_port = 1463
    @timeout = 0.2

    @options = {
      :timeout => @timeout,
      :retries => 4,
      :server_retry_period => nil,
      :raise => :errback
    }

    @pids = []
    @servers = {}

    start_server!(@real_port)

    # Reset error_handler for each test
    EM.error_handler(nil)
  end

  def start_server!(port)
    pid = Process.fork do
      Signal.trap("INT") { exit }
      Greeter::NonblockingServer.new(port).serve
    end
    @pids << pid
    @servers["127.0.0.1:#{port}"] = pid
    sleep 0.05
  end

  def live_servers
    return @servers.keys
  end

  def fake_servers
    return "127.0.0.1:#{@fake_port}"
  end

  def kill_server!(pid)
    Process.kill("INT", pid)
    Process.wait
    @pids.delete(pid)
  end

  def teardown
    pids = @pids.dup
    pids.each do |pid|
      kill_server!(pid)
    end
  end

  def test_successful_method_call
    callback_called = false
    errback_called = false
    disconnect_on_error_count = 0
    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, live_servers, @options)
      singleton_class = (class << client; self end)

      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        disconnect_on_error_count += 1; super *args
      end

      d = client.greeting("someone")
      d.callback do
        callback_called = true
        EM.stop_event_loop
      end
      d.errback do
        errback_called = true
        EM.stop_event_loop
      end
    end

    assert( callback_called )
    assert( !errback_called )

    assert_equal( 0, disconnect_on_error_count )
  end

  def test_retries_for_connection_timeout
    callback_called = false
    errback_called = false
    disconnect_on_error_count = 0
    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, fake_servers, @options)
      singleton_class = (class << client; self end)

      # disconnect_on_error! is called every time a server related
      # connection error happens. it will be called every try (so, retries + 1)
      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        disconnect_on_error_count += 1; super *args
      end

      d = client.greeting("someone")
      d.callback do
        callback_called = true
        EM.stop_event_loop
      end
      d.errback do
        errback_called = true
        EM.stop_event_loop
      end
    end

    assert( !callback_called )
    assert( errback_called )

    assert_equal( @options[:retries] + 1, disconnect_on_error_count )
  end

  def test_retries_for_method_timeout
    callback_called = false
    errback_called = false
    disconnect_on_error_count = 0
    @options[:retries] = 2
    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, live_servers, @options)
      singleton_class = (class << client; self end)

      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        disconnect_on_error_count += 1; super *args
      end

      d = client.delayed_greeting("someone", 1)
      d.callback do
        callback_called = true
        EM.stop_event_loop
      end
      d.errback do
        errback_called = true
        EM.stop_event_loop
      end
    end

    assert( !callback_called )
    assert( errback_called )

    assert_equal( @options[:retries] + 1, disconnect_on_error_count )
  end

  def test_raise_on_error
    callback_called = false
    errback_called = false
    exception_raised = false
    disconnect_on_error_count = 0
    exception = nil

    @options[:raise] = true

    EM.error_handler do |err|
      exception_raised = true
      exception = err
      EM.stop_event_loop
    end

    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, live_servers, @options)
      singleton_class = (class << client; self end)

      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        disconnect_on_error_count += 1; super *args
      end

      d = client.delayed_greeting("someone", 1)
      d.callback do
        callback_called = true
        EM.stop_event_loop
      end
      d.errback do
        errback_called = true
        EM.stop_event_loop
      end
    end

    assert( exception_raised )

    assert_equal( Thrift::TransportException::TIMED_OUT, exception.type )

    assert( !callback_called )
    assert( !errback_called )

    assert_equal( @options[:retries] + 1, disconnect_on_error_count )
  end

  def test_connection_error_with_multiple_pending_requests
    disconnect_on_error_count = 0
    callback_count = 2

    events = []

    decr_cb_count = lambda do
      callback_count -= 1
      if callback_count == 0
        EM.stop_event_loop
      end
    end

    start_server!(@real_port + 1)

    @options[:timeout] = 2

    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, live_servers, @options)
      singleton_class = (class << client; self end)

      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        disconnect_on_error_count += 1; super *args
      end

      mk_callbacks = lambda do |d, name|
        [:callback, :errback].each do |cb_type|
          d.send(cb_type) do
            decr_cb_count.call
            events << "#{cb_type} #{name}"
          end
        end
      end

      d = client.greeting('greeting')
      events << 'send greeting'
      d.callback do
        events << 'callback greeting'
        server = client.current_server.connection_string
        kill_server!(@servers[server])
      end

      d = client.delayed_greeting("g1", 0.7)
      events << 'send g1'
      mk_callbacks.call(d, "g1")

      d = client.delayed_greeting("g2", 0.4)
      events << 'send g2'
      mk_callbacks.call(d, "g2")
    end

    assert_equal( [
      "send greeting",
      "send g1",
      "send g2",
      "callback greeting",
      "callback g2",
      "callback g1",
    ], events )
    assert_equal( 1, disconnect_on_error_count )
  end
end
