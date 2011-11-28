require File.expand_path('test_helper.rb', File.dirname(__FILE__))
require 'eventmachine'
require 'thrift/transport/event_machine_transport'
require 'thrift_client/event_machine_thrift_client'
require File.expand_path('greeter.eventmachine/greeter', File.dirname(__FILE__))


class ThriftClientTest < Test::Unit::TestCase

  def setup
    @fake_port = 1461
    @real_port = real_port = 1463
    @timeout = 0.2

    @options = {
      :timeout => @timeout,
      :retries => 4,
      :server_retry_period => nil,
      :raise => :errback
    }

    @pid = Process.fork do
      Signal.trap("INT") { exit }
      Greeter::Server.new(real_port).serve
    end
    # Need to give the child process a moment to open the listening socket or
    # we get occasional "could not connect" errors in tests.
    sleep 0.05

    # Reset error_handler for each test
    EM.error_handler(nil)
  end

  def teardown
    Process.kill("INT", @pid)
    Process.wait
  end

  def test_retries_for_connection_timeout
    callback_called = false
    errback_called = false
    times_called = 0
    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, "127.0.0.1:#{@fake_port}", @options)
      singleton_class = (class << client; self end)

      # disconnect_on_error! is called every time a server related
      # connection error happens. it will be called every try (so, retries + 1)
      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        times_called += 1; super *args
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

    assert_equal( @options[:retries] + 1, times_called )
  end

  def test_successful_method_call
    callback_called = false
    errback_called = false
    times_called = 0
    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, "127.0.0.1:#{@real_port}", @options)
      singleton_class = (class << client; self end)

      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        times_called += 1; super *args
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

    assert_equal( 0, times_called )
  end

  def test_retries_for_method_timeout
    callback_called = false
    errback_called = false
    times_called = 0
    @options[:retries] = 2
    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, "127.0.0.1:#{@real_port}", @options)
      singleton_class = (class << client; self end)

      # disconnect_on_error! is called every time a server related
      # connection error happens. it will be called every try (so, retries + 1)
      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        times_called += 1; super *args
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

    assert_equal( @options[:retries] + 1, times_called )
  end

  def test_raise_on_error
    callback_called = false
    errback_called = false
    exception_raised = false
    times_called = 0
    exception = nil

    @options[:raise] = true

    EM.error_handler do |err|
      exception_raised = true
      exception = err
      EM.stop_event_loop
    end

    EM.run do
      client = EventMachineThriftClient.new(Async::Greeter::Client, "127.0.0.1:#{@real_port}", @options)
      singleton_class = (class << client; self end)

      # disconnect_on_error! is called every time a server related
      # connection error happens. it will be called every try (so, retries + 1)
      singleton_class.send :define_method, :disconnect_on_error! do |*args|
        times_called += 1; super *args
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

    assert_equal( @options[:retries] + 1, times_called )
  end

  # method error
  # connection error with multiple pending requests
  # timeout and disconnect?
  # method error with multiple pending requests
  # test pending_connect_timeout

end
