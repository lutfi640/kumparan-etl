show wal_level;

select pg_create_logical_replication_slot('kumparan_cdc', 'test_decoding')

select slot_name, plugin, slot_type, active, confirmed_flush_lsn
from pg_replication_slots;

insert into articles(title, content, author_id)
values('Tes CDC', 'body tes', 144);

select lsn, xid, data
from pg_logical_slot_peek_changes('kumparan_cdc', null, NULL)

delete from articles where title = 'Tes CDC'
;