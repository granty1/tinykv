package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	config *config.Config

	db *badger.DB
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (sar *StandAloneReader) GetCF(cf string, key []byte) (val []byte, err error) {
	item, err := sar.txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	val, err = item.ValueCopy(val)
	return
}

func (sar *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sar.txn)
}

func (sar *StandAloneReader) Close() {
	sar.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opt := badger.DefaultOptions
	opt.Dir = s.config.DBPath
	opt.ValueDir = s.config.DBPath
	db, err := badger.Open(opt)
	if err != nil {
		log.Panic("open badger error:", err.Error())
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandAloneReader{
		txn: s.db.NewTransaction(false),
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, v := range batch {
		pu, ok := v.Data.(storage.Put)
		if !ok {
			continue
		}
		wb.SetCF(pu.Cf, pu.Key, pu.Value)
	}
	return wb.WriteToDB(s.db)
}
