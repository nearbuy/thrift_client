Gem::Specification.new do |s|
  s.name        = 'thrift_client'
  s.version     = '0.8.1'
  s.authors     = ["Evan Weaver", "Ryan King", "Jeff Hodges"]
  s.summary     = "A Thrift client wrapper that encapsulates some common failover behavior."
  s.rubygems_version = ">= 0.8"
  s.license = 'Apache 2.0'

  s.add_dependency "thrift", "~> 0.9.0"

  s.has_rdoc      = true

  dir = File.expand_path(File.dirname(__FILE__))

  s.files = Dir.glob("{lib,spec}/**/*")
  s.test_files = Dir.glob("{test,spec,benchmark}/**/*")
  s.executables =  Dir.glob("{bin}/**/*")
end
