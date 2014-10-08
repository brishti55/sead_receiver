$LOAD_PATH.unshift File.expand_path("../..", __FILE__)
require 'json'
require 'bunny'
require 'receiver'

describe Receiver do
  before(:all) do
    #start test receiver
    @receiver = Receiver.new("test_exchange", "test_key")
    
    @sender_conn = Bunny.new
    @sender_conn.start
    @bag_url = "https://github.com/Data-to-Insight-Center/sead-virtual-archive/raw/d2e71a78ab7fd535223bbf679f567cd1b00a5fb1/SEAD-VA-extensions/services/bagItRestService/src/test/resources/org/seadva/bagit/sample_bag.zip"
    @json = {"message_name" => "bag-stage-location", "correlation_id" => "ea7bd000-df4f-11e3-8b68-0800200c9a66", "protocol" => "http", "location" => "#{@bag_url}"}.to_json
    sender_channel = @sender_conn.create_channel
    sender_exchange = sender_channel.direct("test_exchange")
    sender_exchange.publish(@json, :routing_key => "test_key")
    
    @bag_filename = ""
  end
  
  it "receives a json object that has the link" do
    @receiver.retrieve_msg
    location = @receiver.msg_hash['location']
    
    expect(location).not_to eq(nil)
    expect(location).to eq(@bag_url)
  end
  
  it "downloads the bag" do
    bag_location = @receiver.download_bag
    @bag_filename = bag_location
    expect(File.exists? @bag_filename).to eq(true) 
  end
  
  after(:all) do
    @sender_conn.close
    @receiver.close_queue
    
    system "rm -rf #{@bag_filename}" if File.exists? @bag_filename
  end
  
end