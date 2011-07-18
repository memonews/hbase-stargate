require File.join(File.dirname(__FILE__), "..", "spec_helper")

describe Stargate::Client do

  describe "Init" do

    describe "options" do
      it "should set Accept-Encoding header if HTTP compression is disabled" do
        client = Stargate::Client.new("http://localhost:8080", :http_compression => false)
        client.instance_variable_get("@default_headers")["Accept-Encoding"].should == "identity"
      end

      it "should not set Accept-Encoding header if HTTP compression is enabled" do
        client = Stargate::Client.new("http://localhost:8080", :http_compression => true)
        client.instance_variable_get("@default_headers").should_not have_key("Accept-Encoding")
      end

      it "should default to compression enabled" do
        client = Stargate::Client.new("http://localhost:8080", :http_compression => true)
        client.instance_variable_get("@default_headers").should_not have_key("Accept-Encoding")
      end

    end

  end

end

