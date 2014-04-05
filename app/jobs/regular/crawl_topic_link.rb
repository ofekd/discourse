require 'open-uri'
require 'nokogiri'
require 'excon'

module Jobs
  class CrawlTopicLink < Jobs::Base

    class ReadEnough < Exception; end

    # Retrieve a header regardless of case sensitivity
    def self.header_for(head, name)
      header = head.headers.detect do |k, v|
        name == k.downcase
      end
      header[1] if header
    end

    # Follow any redirects that might exist
    def self.final_url(url, limit=5)
      return if limit < 0

      puts url
      head = Excon.head(url, read_timeout: 20, headers: { "User-Agent" => "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"} )
      puts head.status
      if head.status == 200
        url = nil unless header_for(head, 'content-type') =~ /text\/html/
        return url
      end

      uri = URI(url)
      location = header_for(head, 'location')
      if location
        location = "#{uri.scheme}://#{uri.host}#{location}" if location[0] == "/"
        return final_url(location, limit - 1)
      end

      nil
    end

    # Fetch the beginning of a HTML document at a url
    def self.fetch_beginning(url)
      url = final_url(url)
      return "" unless url

      result = ""
      streamer = lambda do |chunk, remaining_bytes, total_bytes|
        result << chunk

        # Using exceptions for flow control is really bad, but there really seems to
        # be no sane way to get a stream to stop reading in Excon (or Net::HTTP for
        # that matter!)
        raise ReadEnough.new if result.size > 1024
      end
      Excon.get(url, response_block: streamer, read_timeout: 20)
      result

    rescue ReadEnough
      result
    end

    def execute(args)
      raise Discourse::InvalidParameters.new(:topic_link_id) unless args[:topic_link_id].present?
      topic_link = TopicLink.where(id: args[:topic_link_id], internal: false).first
      return if topic_link.blank?

      crawled = false

      result = CrawlTopicLink.fetch_beginning(topic_link.url)
      doc = Nokogiri::HTML(result)
      if doc
        title = doc.at('title').try(:inner_text)
        if title.present?
          title.gsub!(/\n/, ' ')
          title.gsub!(/ +/, ' ')
          title.strip!
          if title.present?
            crawled = topic_link.update_attributes(title: title[0..255], crawled_at: Time.now)
          end
        end
      end
    #rescue Exception
      # If there was a connection error, do nothing
    ensure
      topic_link.update_column(:crawled_at, Time.now) if !crawled && topic_link.present?
    end

  end
end
