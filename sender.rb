#!/usr/bin/env ruby
# encoding: utf-8

require "bunny"
require "json"

conn = Bunny.new
conn.start

bag_url = "https://github.com/Data-to-Insight-Center/sead-virtual-archive/raw/d2e71a78ab7fd535223bbf679f567cd1b00a5fb1/SEAD-VA-extensions/services/bagItRestService/src/test/resources/org/seadva/bagit/sample_bag.zip"

json = {"message_name" => "bag-stage-location", "correlation_id" => "ea7bd000-df4f-11e3-8b68-0800200c9a66", "protocol" => "http", "location" => "#{bag_url}"}.to_json

ch       = conn.create_channel
x        = ch.direct("direct_logs")
severity = ARGV.shift || "info"
msg      = ARGV.empty? ? json : ARGV.join(" ")

x.publish(msg, :routing_key => severity)
puts " [x] Sent '#{msg}'"

conn.close