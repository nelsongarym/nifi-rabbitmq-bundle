#!/usr/bin/env ruby
# encoding: utf-8

require "bunny"
require 'faker'

conn = Bunny.new
conn.start

ch   = conn.create_channel

q    = ch.queue("hello", :durable => true)
1.upto(1) do
  msg = Faker::Lorem.paragraphs.join
  ch.default_exchange.publish(msg, :routing_key => q.name)
  puts " [x] Sent #{msg}"
end
