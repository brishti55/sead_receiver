#!/usr/bin/env ruby
# encoding: utf-8

require "bunny"
require "uri"
require 'json'
require 'open-uri'
require 'fileutils'
require 'rubygems'
require 'zip'
require 'bagit'
#require 'packager/package'


class Receiver

  attr_reader :msg_hash

  def initialize(exchange, key)
    @conn = Bunny.new
    @conn.start
    @ch  = @conn.create_channel
    @x   = @ch.direct(exchange)
    @q   = @ch.queue("", :exclusive => true)
    @q.bind(@x, :routing_key => key) 
    
  end

  def retrieve_msg
    begin
      @q.subscribe(:block => false) do |delivery_info, properties, body|
        @msg_hash = JSON.parse(body)
      end
 
    rescue Interrupt => _
      close_queue
    end 
  end

  def download_bag  
    link = @msg_hash["location"] 
    uuid = @msg_hash["correlation_id"] 


    #download and save the zip file 
    bag_path = "data/#{uuid}"
    FileUtils::mkdir_p bag_path
  
    open(link){|f|
      File.open("#{bag_path}/download.zip", "wb") do |file|
        file.puts f.read
      end
    }

    #unzip the downloaded file
    Zip::File.open("#{bag_path}/download.zip") { |zip_file|
       zip_file.each { |f|
         f_path=File.join("#{bag_path}", f.name)
         FileUtils.mkdir_p(File.dirname(f_path))
         zip_file.extract(f, f_path) unless File.exist?(f_path)
       }
    }
    
    # delete the zip file; we don't need it any more
    File.delete "#{bag_path}/download.zip"
    
    # return new bag 
    BagIt::Bag.new bag_path
    
  end


  def close_queue
    @ch.close if @ch
    @conn.close if @conn
  end
end
