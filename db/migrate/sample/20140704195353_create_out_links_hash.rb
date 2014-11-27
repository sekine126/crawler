class CreateOutLinksHash < ActiveRecord::Migration
  def up
    create_table :out_links_hash, :options => 'ENGINE=InnoDB DEFAULT CHARSET=utf8' do |t|
      t.string :base_url_hash, :null => false
      t.string :out_link_hash, :null => false
      t.timestamp :created, :null => false
    end
    execute 'ALTER TABLE out_links_hash CHANGE created created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
  end

  def down
    drop_table :out_links_hash
  end
end
