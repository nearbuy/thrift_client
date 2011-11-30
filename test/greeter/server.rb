module Greeter
  class Handler
    def greeting(name)
      "hello there #{name}!"
    end

    def delayed_greeting(name, sleep_seconds)
      sleep sleep_seconds
      return greeting(name)
    end

    def yo(name)
      #whee
    end
  end

  class Server
    def initialize(port)
      @port = port
      handler = Greeter::Handler.new
      processor = Greeter::Processor.new(handler)
      transport = Thrift::ServerSocket.new("127.0.0.1", port)
      transportFactory = Thrift::FramedTransportFactory.new()
      @server = server_class.new(processor, transport, transportFactory)
    end

    def server_class
      Thrift::SimpleServer
    end

    def serve
      @server.serve()
    end
  end

  class NonblockingServer < Server
    def server_class
      Thrift::NonblockingServer
    end
  end

  # client:
  # trans = Thrift::HTTPClientTransport.new("http://127.0.0.1:9292/greeter")
  # prot = Thrift::BinaryProtocol.new(trans)
  # c = Greeter::Client.new(prot)
  class HTTPServer
    def initialize(uri)
      uri = URI.parse(uri)
      handler = Greeter::Handler.new
      processor = Greeter::Processor.new(handler)
      path = uri.path[1..-1]
      @server = Thrift::MongrelHTTPServer.new(processor, :port => uri.port, :ip => uri.host, :path => path)
    end

    def serve
      @server.serve()
    end
  end
end
