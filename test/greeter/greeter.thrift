namespace rb.eventmachine Async

service Greeter {
  string greeting(1:string name)
  string delayed_greeting(1:string name, 2:i32 sleep_seconds)
  oneway void yo(1:string name)
}
