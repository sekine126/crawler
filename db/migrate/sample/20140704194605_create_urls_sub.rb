class CreateUrlsSub < ActiveRecord::Migration
  def up
    create_table :urls_sub, :options => 'ENGINE=InnoDB DEFAULT CHARSET=utf8' do |t|
      t.string :url, :null => false
      t.string :url_hash, :null => false
      t.timestamp :created, :null => false
    end
    execute 'ALTER TABLE urls_sub CHANGE created created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
  end

  def down
    drop_table :urls_sub
  end
end
