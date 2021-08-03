# Logstash

This example joins data from two tcp sockets opened by logstash in mode server.
The codec is to have logstash add newlines at the end of the event written on the socket
(otherwise Spark doesn't treat the message correctly).

# Logstash config

In a logstash pipeline add in output plugin

  tcp {
    id => "tcp_output_1"
    host => "localhost"
    port => 8898
    mode => "server"
    codec => json_lines
  }




In a logstash pipeline add in output plugin add in output plugin

  tcp {
    id => "tcp_output_2"
    host => "localhost"
    port => 8888
    mode => "server"
    codec => json_lines
  }