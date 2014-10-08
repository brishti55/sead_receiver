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
    @q.subscribe(:block => false) do |delivery_info, properties, body|
      @msg_hash = JSON.parse(body)
    end
  end

  def download_bag  
    link = @msg_hash["location"] 
    uuid = @msg_hash["correlation_id"] 


    #download and save the zip file 
    FileUtils::mkdir_p "data/#{uuid}"
  
    open(link){|f|
      File.open("data/#{uuid}/download.zip", "wb") do |file|
        file.puts f.read
      end
    }

    #unzip the downloaded file
    
    Zip::File.open("data/#{uuid}/download.zip") { |zip_file|
       zip_file.each { |f|
         f_path=File.join("data/#{uuid}/download", f.name)
         FileUtils.mkdir_p(File.dirname(f_path))
         zip_file.extract(f, f_path) unless File.exist?(f_path)
       }
    }
    
    return "data/#{uuid}/download"
    
  end

  def do_work
    begin
  
        #validating an existing bag

        existing_base_path = "data/#{uuid}/download"
     
        bag = BagIt::Bag.new existing_base_path

        if bag.valid?
          puts "#{existing_base_path} is valid"
        else
          puts "#{existing_base_path} is not valid"
        end
    

    rescue Exception => e
     puts e.message
    rescue Interrupt => _
      close_queue
    ensure
      close_queue
    end
  end

  def close_queue
    @ch.close if @ch
    @conn.close if @conn
  end
end
