require 'yaml'
require 'nokogiri'
require 'addressable/uri'
require 'open-uri'
require 'open_uri_redirections'
require 'uri'
require 'mysql'
require 'digest/md5'
require 'rubygems'
require 'thread'
require 'timeout'
require 'cgi'
require 'robots'
require 'logger'
require 'fileutils'
# require 'ruby-prof'

class Crawler

  attr_accessor :main_queue, :high_priority_queue, :high_priority_queue_tmp, :low_priority_queue,
  :low_priority_queue_tmp, :start_urls, :repeat_urls, :start_time_pre_queue,
  :interval_minute_pre_queue, :interval_minute, :level, :stop_loop_flag, :start_time

  def initialize(log, execute_time, file_id, sql_path, config)
    # インスタンス変数を初期化
    @log = log
    @execute_time = execute_time
    @file_id = file_id
    @sql_path = sql_path
    @mysql_config = config['mysql_config']
    @timeout_sec = config['timeout_sec']
    @xpath_for_html = config['xpath_for_html']
    @xpath_for_xml = config['xpath_for_xml']
    @skip_extensions = config['skip_extensions']
    @skip_protocols = config['skip_protocols']
    @finish_minute = config['finish_minute']

    # 実行フラグをfalseで初期化
    @stop_loop_flag = false
    @finish_flag = false

    # 巡回済みのurl配列を生成
    @crawled_urls = Array.new

    # キュー生成
    @main_queue = Queue.new
    @high_priority_queue = Queue.new
    @low_priority_queue = Queue.new
    @high_priority_queue_tmp = Queue.new
    @low_priority_queue_tmp = Queue.new

    # スタートurl配列を生成
    @start_urls = Array.new

    # 繰り返しクロールするurl配列を生成
    @repeat_urls = Array.new
  end

  def crawl(queue_flag, robots, max_threads, min_delay_sec, max_delay_sec, priority_crawl_regexp)
    # Queueのサイズを表示
    p "#{queue_flag}.size : #{@main_queue.size}" if $DEBUG
    @log.info("#{queue_flag}.size : #{@main_queue.size}")

    # insertされたurlのカウント用
    count_insert_urls = 0

    # マルチスレッド用の複数のDBコネクションを格納する配列
    db_connects = Array.new

    # マルチスレッド処理
    Array.new(max_threads) do |thread_i|
      Thread.new { # スレッド作成
        begin
          loop do
            begin

              # 終了時間のチェック
              check_finishtime
              break if @finish_flag

              # スタートページ再設定時間のチェック
              check_breaktime(queue_flag)
              if @stop_loop_flag
                p 'time is over.(break)'
                @log.info('time is over.(break)')
              end
              break if @stop_loop_flag || @main_queue.empty?

              # urlをキューから取り出す
              url = @main_queue.pop(true)

              # キューの最後なのでbreak
              break if url == nil

              # スタートページかクロール済みのurlであればnext
              next if (@start_urls.include?(url) || @crawled_urls.include?(url)) && (queue_flag =='q' || queue_flag =='hpreqt' || queue_flag =='lpq')

              # robots.txtをチェック
              unless robots.allowed?(url)
                @log.warn("[#{url}] disallow in robots.txt")
                next
              end

              # ランダム時間スリープ
              p 'random sleep.' if $DEBUG
              sleep(min_delay_sec + rand * (max_delay_sec - min_delay_sec))

              # ページにアクセス
              page_info = open_html(url)

              if page_info.nil?
                @log.warn("[#{url}](next)")
                @crawled_urls.push(url)
                next
              end

              # リダイレクト先のurlを取得
              redirect_url = page_info.base_uri.to_s

              ### クロール処理
              begin
                # まだクロールしていないページならばクロールする
                unless @crawled_urls.include?(redirect_url)
                  # 出リンクを取得
                  outlinks = fetch_outlinks(page_info)

                  p "queue_flag: #{queue_flag}" if $DEBUG
                  p "priority_crawl_regexp: #{priority_crawl_regexp.inspect}" if $DEBUG

                  # preq, hpqtの場合は深い階層のクロールを行わない
                  unless queue_flag == 'preq' || queue_flag == 'hpqt'
                    outlinks.each { |outlink|
                      # 正規表現によって優先的にクロールするページとしないページを分ける
                      if (priority_crawl_regexp === outlink) && (queue_flag != 'lpq')
                        @high_priority_queue.push(outlink)
                        p "@high_priority_queue.push << #{outlink}" if $DEBUG
                      else
                        @low_priority_queue.push(outlink)
                        p "@low_priority_queue.push << #{outlink}" if $DEBUG
                      end
                    }
                  end

                  db_connects[thread_i] = mysql_connect unless queue_flag == 'preq'

                  ### DBへの保存処理
                  # 繰り返しクロールするurlはDBへ保存しない
                  unless @repeat_urls.include?(redirect_url)
                    @crawled_urls.push(redirect_url)
                    write_outlink_data(redirect_url, outlinks, queue_flag)
                    write_outlinkhash_data(redirect_url, outlinks, queue_flag)
                    insert_url(redirect_url, db_connects[thread_i], queue_flag) if queue_flag != 'preq'
                    count_insert_urls += 1
                  end

                  db_connects[thread_i].close unless queue_flag == 'preq'

                  sleep(10)
                end

              rescue => e
                p e.message
                @log.error(e.message)
              end # クロール処理

            rescue => e
              p e.message
              @log.error(e.message)
              next
            end

          end # loop

          p 'end while' if $DEBUG

          # 最後を表すnilを別スレッドのために残しておく
          @main_queue.push nil unless queue_flag == 'hpqt'
          p 'main_queue pushed.' if $DEBUG
        end
      }
    end.each(&:join)

    p 'end each' if $DEBUG

    # 実行終了時に出リンクデータをDBへインサートしてから終了する
    if @finish_flag
      @log.info('---- FINISH TIME ----')
      p '---- FINISH TIME ----'
      insert_sqlpool
      exit
    end

    # main_queueに残ったurlをhigh_priority_queue_tmpへ避難させておく
    while !@main_queue.empty? && (queue_flag == 'q' || queue_flag == 'hpqt')
      remain_url = @main_queue.pop(true)
      @high_priority_queue_tmp.push(remain_url) unless remain_url == nil
    end

    puts "count_insert_urls = #{count_insert_urls}"
    @log.info("count_insert_urls = #{count_insert_urls}")

    finish_message = "finish #{queue_flag}"
    finish_message += "[level:#{@level}]" if queue_flag == 'q'

    @log.info(finish_message)
    p finish_message
  end

  def mysql_connect
    p '[mysql_connect]' if $DEBUG
    my = Mysql.new(@mysql_config['host'], @mysql_config['user'], @mysql_config['password'], @mysql_config['db_name'])
    my.charset = 'utf8'
    return my
  end

  def write_outlink_data(base_url, outlinks, queue_flag)
    p '[write_outlink_data]' if $DEBUG

    if queue_flag == 'lpq'
      table_name = 'out_links_sub'
      sub_suffix = '_sub'
    else
      table_name = 'out_links'
      sub_suffix = ''
    end

    sql = "INSERT INTO #{table_name} (base_url, out_link) VALUES "
    outlinks.each do |outlink|
      sql += "(\'#{CGI.escape(base_url)}\', \'#{CGI.escape(outlink)}\'),"
    end

    # sqlの一番後ろの,を;へ置換
    sql[-1] = ';'

    File.open("#{@sql_path}sqlpool_#{@execute_time.strftime('%Y%m%d')}_#{@file_id}#{sub_suffix}.txt", 'a+') do |f|
      f.puts(sql)
    end
  end

  def write_outlinkhash_data(base_url, outlinks, queue_flag)
    p '[write_outlinkhash_data]' if $DEBUG

    if queue_flag == 'lpq'
      table_name = 'out_links_hash_sub'
      sub_suffix = '_sub'
    else
      table_name = 'out_links_hash'
      sub_suffix = ''
    end

    base_hash = Digest::MD5.new
    base_hash.update(CGI.escape(base_url))
    sql = "INSERT INTO #{table_name} (base_url_hash, out_link_hash) VALUES "

    outlinks.each do |outlink|
      out_hash = Digest::MD5.new
      out_hash.update(CGI.escape(outlink))
      sql += "(\'#{base_hash.hexdigest}\', \'#{out_hash.hexdigest}\'),"
    end

    # sqlの一番後ろの,を;へ置換
    sql[sql.length - 1] = ';'

    File.open("#{@sql_path}sqlpool_hash_#{@execute_time.strftime('%Y%m%d')}_#{@file_id}#{sub_suffix}.txt", 'a+') do |f|
      f.puts(sql)
    end
  end

  def insert_url(url, my, queue_flag)
    p '[insert_url]' if $DEBUG

    url_hash = Digest::MD5.new
    url_hash.update(CGI.escape(url))

    if queue_flag == 'lpq'
      table_name = 'urls_sub'
      next_table_name = 'urls'
    else
      table_name = 'urls'
      next_table_name = 'urls_sub'
    end

    sql = "select count(*) from #{table_name} where url_hash = ?"
    stmt = my.prepare(sql)
    res = stmt.execute url_hash.hexdigest

    res.each do |row|
      if row[0] == 0
        sql = "select count(*) from #{next_table_name} where url_hash = ?"
        stmt = my.prepare(sql)
        res2 = stmt.execute url_hash.hexdigest

        res2.each do |row2|
          if row2[0] == 0
            sql = "INSERT INTO #{table_name} (url, url_hash) VALUES (?, ?)"
            stmt = my.prepare(sql)
            stmt.execute CGI.escape(url), url_hash.hexdigest
          end
        end
      end
    end
  end

  def open_html(url)
    p '[open_html]' if $DEBUG

    @log.error('URL is nil or Empty!') if url.nil? || url.empty?

    uri = Addressable::URI.parse(url)

    begin
      page_info = timeout(@timeout_sec){ open(uri.normalize.to_s, :allow_redirections => :all) }
      return page_info
    rescue TimeoutError => e
      @log.warn("[open_html] [#{uri.normalize}] Time out!")
      return nil
    rescue => e
      @log.error("[open_html] [#{uri.normalize}] #{e.message}")
      return nil
    end
  end

  def fetch_outlinks(page_info)
    p '[fetch_outlinks]' if $DEBUG

    url = page_info.base_uri.to_s

    if page_info.content_type == 'text/html'
      doc = Nokogiri::HTML(page_info)
      path = @xpath_for_html
    elsif page_info.content_type == 'text/xml'
      doc = Nokogiri::XML(page_info)
      path = @xpath_for_xml
    else
      doc = Nokogiri::HTML(page_info)
    end

    outlinks = Array.new

    # Search for nodes by xpath
    doc.xpath(path).each do |link|
      # 拡張子とプロトコルの確認の確認
      if @skip_extensions === link
        p "[fetch_outlinks] #{link} (next)" if $DEBUG
        @log.info("[fetch_outlinks] #{link} (next)")
        next
      elsif @skip_protocols === link
        p "[fetch_outlinks] #{link} (next)" if $DEBUG
        @log.info("[fetch_outlinks] #{link} (next)")
        next
      end

      # 絶対パスを結果の配列に追加
      outlinks.push(URI.join(url, link).to_s) rescue next
    end

    return outlinks
  end

  def check_finishtime
    p '[check_finishtime]' if $DEBUG

    now_time = Time.now
    elapsed_sec = now_time - @execute_time

    @finish_flag = elapsed_sec > (@finish_minute * 60)
  end

  def check_breaktime(queue_flag)
    p '[check_breaktime]' if $DEBUG

    now_time = Time.now
    if queue_flag == 'preq'
      elapsed_sec = now_time - @start_time_pre_queue
      interval_minute = @interval_minute_pre_queue
    else
      elapsed_sec = now_time - @start_time
      interval_minute = @interval_minute
    end

    @stop_loop_flag = elapsed_sec > interval_minute
  end

  def load_lasttime_urls(my)
    p '[load_lasttime_urls]' if $DEBUG

    res = Array.new
    2.times do |time|
      case time
      when 0 then
        sql = 'select url from urls'
      when 1 then
        sql = 'select url from urls_sub'
      end
      stmt = my.prepare(sql)
      res.push(stmt.execute)
    end
    return res
  end

  def take_shelter_queue
    p '[take_shelter_queue]' if $DEBUG

    # low_priority_queueで余ったURLを@low_priority_queue_tmpへpush(1000件まで)
    remain_urls_size = @low_priority_queue.size
    while !@low_priority_queue.empty? && (@low_priority_queue.size > remain_urls_size - 1000)
      url = @low_priority_queue.pop(true)
      @low_priority_queue_tmp.push(url) unless url == nil
    end
  end

  # start_urls, repeat_urlsで読み込むURLをファイルから取得する
  def load_urls(file_path)
    p '[load_urls]' if $DEBUG

    urls = Array.new
    open(file_path) do |f|
      f.each do |line|
        urls.push(line.chomp!)
        puts "[load_urls] #{line}" if $DEBUG
      end
    end
    return urls
  end

  def insert_sqlpool
    p '[insert_sqlpool]' if $DEBUG

    p '***** insert sqlpool START *****'
    @log.info('***** insert sqlpool START *****')

    # insertファイル名
    out_links_file = "#{@sql_path}sqlpool_#{@execute_time.strftime('%Y%m%d')}_#{@file_id}.txt"
    out_links_sub_file = "#{@sql_path}sqlpool_#{@execute_time.strftime('%Y%m%d')}_#{@file_id}_sub.txt"
    out_links_hash_file = "#{@sql_path}sqlpool_hash_#{@execute_time.strftime('%Y%m%d')}_#{@file_id}.txt"
    out_links_hash_sub_file = "#{@sql_path}sqlpool_hash_#{@execute_time.strftime('%Y%m%d')}_#{@file_id}_sub.txt"

    sleep(5)

    # mysql接続
    my = mysql_connect

    # データを挿入する処理
    insert_file(out_links_file, my)
    insert_file(out_links_sub_file, my)
    insert_file(out_links_hash_file, my)
    insert_file(out_links_hash_sub_file, my)

    p '***** insert sqlpool FINISH *****'
    @log.info('***** insert sqlpool FINISH *****')
  end

  def insert_file(insert_file, my)
    p '[insert_file]' if $DEBUG

    if File.exist?(insert_file)
      begin

        line_num = 1
        puts "insert_file: #{insert_file}"
        @log.info("insert_file: #{insert_file}")

        # 1行ずつinsert文を実行していく処理
        File.open(insert_file, 'r') do |f|
          while line = f.gets
            begin
              my.query(line)
            rescue => e
              p "[L#{line_num}]#{e.message}"
              @log.error("[L#{line_num}]#{e.message}")
            ensure
              line_num += 1
            end
          end
        end

      rescue => e
        p e.message
        @log.error("[insert_file]#{e.message}")
      end
    end

  end

end # class

####################################
#            実行部分              #
####################################

raise ArgumentError, 'please set file_id!!' if ARGV[0].nil?

p '***** crawler setting start *****' if $DEBUG

# 実行開始時間
execute_time = Time.now
p "execute_time: #{execute_time}" if $DEBUG

# ファイル識別子をコマンドライン引数から取得
file_id = ARGV[0]
p "file_id: #{file_id}" if $DEBUG

# プロジェクトのホームパスを取得
home_path = File.expand_path(File.dirname(__FILE__))
home_path.slice!(/\/src$/)
p "home_path: #{home_path}" if $DEBUG

# ログファイルの保存ディレクトリの指定
log_path = "#{home_path}/log/"
p "log_path: #{log_path}" if $DEBUG

# sqlファイルの保存ディレクトリの指定
sql_path = "#{home_path}/sql/"
p "sql_path: #{sql_path}" if $DEBUG

# 設定ファイルをロード
config = YAML.load_file("#{home_path}/config/config_crawler_#{file_id}.yml")
p 'config_file loaded.' if $DEBUG

# ログ
log = Logger.new("#{log_path}#{execute_time.strftime('%Y%m%d')}_#{file_id}.log")
log.level = Logger::INFO

# robots.txt
robots = Robots.new config['robots_user_agent']

# スタートページのURLリストのファイルパス
start_urls_path = "#{home_path}/config/#{config['start_urls_file']}"
p "#{config['start_urls_file']} loaded." if $DEBUG

# 何度でも巡回できるURLリストのファイルパス
repeat_urls_path = "#{home_path}/config/#{config['repeat_urls_file']}"
p "#{config['repeat_urls_file']} loaded." if $DEBUG

p '---- START ----' if $DEBUG
log.info('---- START ----')

# クラス生成
crawler = Crawler.new(log, execute_time, file_id, sql_path, config)

# DB接続
my = crawler.mysql_connect

# キュー生成
main_queue = Queue.new
high_priority_queue = Queue.new
low_priority_queue = Queue.new
high_priority_queue_tmp = Queue.new
low_priority_queue_tmp = Queue.new

# スタートページのURLリストを取得してset
crawler.start_urls = crawler.load_urls(start_urls_path)
p 'start_urls loaded.' if $DEBUG

# 何度でも巡回できるURLリストを取得してset
crawler.repeat_urls = crawler.load_urls(repeat_urls_path)
p 'repeat_urls loaded.' if $DEBUG

################ 前回のページを回る処理 ################

puts 'START to CRAWL last time crawled urls.'

# 前回に回ったurlの取得
# HACK: 配列を2重に処理しているのが分かりにくい
lasttime_urls = crawler.load_lasttime_urls(my)
lasttime_urls.each do |urls|
  urls.each do |url|
    main_queue.push(CGI.unescape(url[0]))
  end
end

main_queue.push(nil)

# pre_queueのスタート時間をset
crawler.start_time_pre_queue = Time.now
p "start_time_pre_queue: #{crawler.start_time_pre_queue}" if $DEBUG

# pre_queueの実行時間をset
crawler.interval_minute_pre_queue = config['interval_minute_pre_queue'] * 60
p "interval_minute_pre_queue: #{crawler.interval_minute_pre_queue}" if $DEBUG

# set
crawler.main_queue = main_queue

crawler.crawl('preq', robots, config['max_threads'], config['min_delay_sec'], config['max_delay_sec'], config['priority_crawl_regexp'])

puts 'FINISH to CRAWL last time crawled urls.'

################ ここまで前日のページを回る処理 ##############

# whileの実行を新しくする指定時間（分×60）をset
crawler.interval_minute = config['interval_minute'] * 60
p "interval_minute: #{crawler.interval_minute}" if $DEBUG

loop do
  main_queue.clear
  high_priority_queue.clear
  low_priority_queue.clear

  # loopの開始時刻をset
  crawler.start_time = Time.now
  # loopのstopフラグをfalseにset
  crawler.stop_loop_flag = false

  # low_priority_queue_tmpへ避難していたページをlow_priority_queueへ追加
  # HACK: popしてpushするメソッドを作る
  while !low_priority_queue_tmp.empty?
    url = low_priority_queue_tmp.pop(true)
    low_priority_queue.push(url) unless url == nil
  end

  crawler.start_urls.each do |start_url|
    main_queue.push(start_url)
  end

  main_queue.push(nil)

  # set
  crawler.main_queue = main_queue
  crawler.high_priority_queue = high_priority_queue
  crawler.low_priority_queue = low_priority_queue

  crawler.crawl('stq', robots, config['max_threads'], config['min_delay_sec'], config['max_delay_sec'], config['priority_crawl_regexp'])

  # mainでurlをpushしたQueueをget
  high_priority_queue = crawler.high_priority_queue
  low_priority_queue = crawler.low_priority_queue

  2.upto(config['depth_limit']) do |level|
    p "level: #{level}" if $DEBUG

    crawler.level = level
    main_queue.clear

    while !high_priority_queue.empty?
      url = high_priority_queue.pop(true)
      main_queue.push(url) unless url == nil
    end

    high_priority_queue.clear
    crawler.high_priority_queue = high_priority_queue

    main_queue.push(nil)

    # set
    crawler.main_queue = main_queue
    crawler.high_priority_queue = high_priority_queue
    crawler.low_priority_queue = low_priority_queue
    crawler.high_priority_queue_tmp = high_priority_queue_tmp

    p 'main q START' if $DEBUG
    crawler.crawl('q', robots, config['max_threads'], config['min_delay_sec'], config['max_delay_sec'], config['priority_crawl_regexp'])
    p 'main q FINISH' if $DEBUG

    # mainでurlをpushしたQueueをget
    high_priority_queue = crawler.high_priority_queue
    low_priority_queue = crawler.low_priority_queue
    high_priority_queue_tmp = crawler.high_priority_queue_tmp

  end # depth_limit.times

  # 最後のhpq内のurlを次回以降の余り時間で処理するために避難させておく
  while !high_priority_queue.empty?
    url = high_priority_queue.pop(true)
    high_priority_queue_tmp.push(url) unless url == nil
  end

  # 経過時間が指定時間より短い場合は、high_priority_queue_tmpとlow_priority_queueを実行する
  elapsed_sec = Time.now - crawler.start_time # 経過時間

  if elapsed_sec < crawler.interval_minute
    p "#{((crawler.interval_minute - elapsed_sec) / 60).round} minute remaining."
    log.info("#{((crawler.interval_minute - elapsed_sec) / 60).round} minute remaining.")

    main_queue.clear

    while !high_priority_queue_tmp.empty?
      url = high_priority_queue_tmp.pop(true)
      main_queue.push(url) unless url == nil
    end

    # set
    crawler.main_queue = main_queue
    crawler.high_priority_queue = high_priority_queue
    crawler.low_priority_queue = low_priority_queue
    crawler.high_priority_queue_tmp = high_priority_queue_tmp

    crawler.crawl('hpqt', robots, config['max_threads'], config['min_delay_sec'], config['max_delay_sec'], config['priority_crawl_regexp'])

    # mainでurlをpushしたQueueをget
    high_priority_queue = crawler.high_priority_queue
    low_priority_queue = crawler.low_priority_queue
    high_priority_queue_tmp = crawler.high_priority_queue_tmp

    main_queue.clear

    while !low_priority_queue.empty?
      url = low_priority_queue.pop(true)
      main_queue.push(url) unless url == nil
    end

    # set
    crawler.main_queue = main_queue
    crawler.low_priority_queue = low_priority_queue

    crawler.crawl('lpreq', robots, config['max_threads'], config['min_delay_sec'], config['max_delay_sec'], config['priority_crawl_regexp'])

    # mainでurlをpushしたQueueをget
    low_priority_queue = crawler.low_priority_queue
    main_queue = crawler.main_queue

    while !main_queue.empty?
      url = main_queue.pop(true)
      low_priority_queue.push(url) unless url == nil
    end

  end

  low_priority_queue_tmp.clear
  p 'low_priority_queue_tmp cleared.' if $DEBUG

  # set
  crawler.low_priority_queue = low_priority_queue
  crawler.low_priority_queue_tmp = low_priority_queue_tmp

  # 1000件まで避難させる
  crawler.take_shelter_queue

  # get
  low_priority_queue = crawler.low_priority_queue
  low_priority_queue_tmp = crawler.low_priority_queue_tmp

  p "low_priority_queue_tmp.size: #{low_priority_queue_tmp.size}" if $DEBUG

end # loop